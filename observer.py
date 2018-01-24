import argparse
import functools
import json
import mvg_api
import mvv_api
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def timeit(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        result = func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        if elapsedTime < 60:
            print('{} finished in {}s'.format(func.__name__, round(elapsedTime, 2)))
        else:
            mins = int(elapsedTime / 60)
            secs = round(elapsedTime % 60, 2)
            if mins < 60:
                print('{} finished in {}m {}s'.format(func.__name__, mins, secs))
            else:
                hours = int(duration / 3600)
                mins = mins % 60
                print('{} finished in {}h {}m {}s'.format(func.__name__, hours, mins, secs))

        return result
    return newfunc

class AtomicCounter:
    def __init__(self, initial=0):
        """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value

class Observer:
    def __init__(self):
        self.mvgapi = mvg_api.MVGAPI()
        self.mvvapi = mvv_api.MVVAPI()

    def connect(self, connector): # TODO
        self.connection = connector.connect(host=os.environ['HOST'],
                                            user=os.environ['USER'],
                                            password=os.environ['PASSWORD'],
                                            db=os.environ['DB'],
                                            charset='utf8',
                                            cursorclass=connector.cursors.DictCursor)

    def disconnect(self):
        self.connection.close()

    #@timeit
    def refresh_stations(self, stations=None):
        inserted = 0
        changed = 0

        if not stations:
            stations = self.mvgapi.get_stations().stations

        with self.connection.cursor() as cursor:
            sql_insert_station = """
                INSERT INTO station (station_id, type, name, aliases, hasLiveData, hasZoomData, place, longitude, latitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY
                UPDATE station_id=%s, type=%s, name=%s, aliases=%s, hasLiveData=%s, hasZoomData=%s, place=%s, longitude=%s, latitude=%s
            """
            sql_insert_station_product = """
                INSERT INTO station_product
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE station_id=station_id
            """

            for station in stations:
                cursor.execute(sql_insert_station,
                               (station.id, station.type, station.name, station.aliases, station.has_live_data, station.has_zoom_data, station.place, station.longitude, station.latitude,
                                station.id, station.type, station.name, station.aliases, station.has_live_data, station.has_zoom_data, station.place, station.longitude, station.latitude))

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    changed += 1

                for product in station.products:
                    cursor.execute(sql_insert_station_product, (station.id, product))

        self.connection.commit()

        return inserted, changed

    @timeit
    def refresh_zoom_data(self):
        inserted = 0

        with self.connection.cursor() as cursor:
            sql_select_stations_with_zoom_data = """
                SELECT station_id
                FROM station
                WHERE hasZoomData = 1
            """
            sql_select_device_status = """
                SELECT status
                FROM transport_device
                WHERE station_id = %s AND xcoordinate = %s AND ycoordinate = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            sql_insert_device = """
                INSERT INTO transport_device (station_id, type, identifier, name, description, status, oos_since, oos_until, oos_description, timestamp, xcoordinate, ycoordinate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(sql_select_stations_with_zoom_data)
            stations = cursor.fetchall()
            for row in stations:
                zoom = self.mvgapi.get_zoom_data(row['station_id'])
                for transport_device in zoom.transport_devices:
                    cursor.execute(sql_select_device_status, (row['station_id'], transport_device.xcoordinate, transport_device.ycoordinate))
                    latest_entry = cursor.fetchone()

                    if not latest_entry or latest_entry['status'] != transport_device.status:
                        cursor.execute(sql_insert_device, (zoom.station_id,
                                                           transport_device.type,
                                                           transport_device.identifier,
                                                           transport_device.name,
                                                           transport_device.description,
                                                           transport_device.status,
                                                           transport_device.oos_since,
                                                           transport_device.oos_until,
                                                           transport_device.oos_description,
                                                           transport_device.last_update,
                                                           transport_device.xcoordinate,
                                                           transport_device.ycoordinate))
                        inserted += 1
                time.sleep(0.3)

        self.connection.commit()

        return inserted

    def load_schedule(self, lock, cursor, counter, station_id, name):
        print('schedule: ' + name)

        departures = self.mvvapi.get_departures(name, limit=30)

        sql_insert_schedule = """
            INSERT INTO schedule
            VALUES (%s, %s, (SELECT station_id
                             FROM station
                             WHERE MATCH (name) AGAINST (%s IN NATURAL LANGUAGE MODE)
                             ORDER BY (CASE WHEN
                                           name = %s THEN 1 ELSE 0
                                       END) DESC,
                                      (MATCH (name) AGAINST (%s IN NATURAL LANGUAGE MODE)) DESC
                             LIMIT 1),
                    %s, %s, %s)
        """

        sql_data = []
        for departure in departures:
            if departure.product != 'UBAHN' and departure.product != 'BUS' and departure.product != 'STADTBUS':
                continue

            sql_data.append((station_id,
                             departure.mvv_station_id,
                             departure.destination,
                             departure.destination,
                             departure.destination,
                             departure.departure_time,
                             departure.product,
                             departure.label))

        try:
            lock.acquire()
            rowcount = cursor.executemany(sql_insert_schedule, sql_data)

            counter.increment(rowcount)
        except pymysql.err.IntegrityError as e:
            pass
        except Exception as e:
            obj = {
                'status': 'ERROR',
                'module': 'schedule',
                'message': str(e)
            }
            print(json.dumps(obj))
        finally:
            lock.release()

    # Every 20 minutes
    @timeit
    def load_schedule_threaded(self):
        executor = ThreadPoolExecutor(max_workers=5)
        lock = threading.Lock()

        inserted = AtomicCounter()
        with self.connection.cursor() as cursor:
            sql_select_stations_with_live_data = """
                SELECT station_id, name
                FROM station
                WHERE hasLiveData = 1
            """

            cursor.execute(sql_select_stations_with_live_data)
            stations = cursor.fetchall()
            for row in stations:
                executor.submit(self.load_schedule, lock, cursor, inserted, row['station_id'], row['name'])

            executor.shutdown(wait=True)

        self.connection.commit()

        return inserted.value

    def load_departures(self, lock, cursor, counter, station_id, name):
        print('departure: ' + name)

        departures = self.mvgapi.get_departures_list(station_id)

        sql_insert_departure = """
            INSERT INTO departure
            VALUES (%s, (SELECT station_id
                         FROM station
                         WHERE name = %s
                         LIMIT 1),
                    %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            lock.acquire()
            rowcount = cursor.executemany(sql_insert_departure, departures)

            counter.increment(rowcount)
        except pymysql.err.IntegrityError as e:
            pass
        except Exception as e:
            obj = {
                'status': 'ERROR',
                'module': 'departure',
                'message': str(e)
            }
            print(json.dumps(obj))
        finally:
            lock.release()

    # Every 5 minutes
    @timeit
    def load_departures_threaded(self):
        executor = ThreadPoolExecutor(max_workers=5)
        lock = threading.Lock()

        inserted = AtomicCounter()
        with self.connection.cursor() as cursor:
            sql_select_stations_with_live_data = """
                SELECT station_id, name
                FROM station
                WHERE hasLiveData = 1
            """

            cursor.execute(sql_select_stations_with_live_data)
            stations = cursor.fetchall()
            for row in stations:
                executor.submit(self.load_departures, lock, cursor, inserted, row['station_id'], row['name'])

            executor.shutdown(wait=True)

        self.connection.commit()

        return inserted.value

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--connector', dest='connector', default='pymysql')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--refresh-stations', dest='refresh_stations', action='store_true')
    group.add_argument('-z', '--refresh-zoom', dest='refresh_zoom', action='store_true')
    group.add_argument('-s', '--refresh-schedule', dest='refresh_schedule', action='store_true')
    group.add_argument('-d', '--refresh-departures', dest='refresh_departures', action='store_true')
    args = parser.parse_args()

    observer = Observer()
    if args.connector == 'pymysql':
        import pymysql
        observer.connect(pymysql)
    else:
        print('Unknown connector specified.')
        return

    if args.refresh_stations:
        print('Refreshing stations...')
        inserted, changed = observer.refresh_stations()
        print('Stations inserted: ' + str(inserted) + ', changed: ' + str(changed))
    elif args.refresh_zoom:
        print('Refreshing zoom data...')
        inserted = observer.refresh_zoom_data()
        print('Zoom devices inserted: ' + str(inserted))
    elif args.refresh_schedule:
        print('Loading schedule...')
        inserted = observer.load_schedule_threaded()
        print('Schedule items inserted: ' + str(inserted))
    elif args.refresh_departures:
        print('Loading departures...')
        inserted = observer.load_departures_threaded()
        print('Departure items inserted: ' + str(inserted))

    observer.disconnect()

if __name__ == "__main__":
    main()