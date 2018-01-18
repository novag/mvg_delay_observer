# =====================================================
# Copyright (c) 2017 novag All Rights Reserved.
# 
# Confidential and Proprietary - novag
# =====================================================

import argparse
import json
import mvg_api
import mvv_api
import os
import pymysql
import threading
import time
from concurrent.futures import ThreadPoolExecutor


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

    def connect(self):
        self.connection = pymysql.connect(host=os.environ['HOST'],
                                          user=os.environ['USER'],
                                          password=os.environ['PASSWORD'],
                                          db=os.environ['DB'],
                                          charset='utf8',
                                          cursorclass=pymysql.cursors.DictCursor)

    def disconnect(self):
        self.connection.close()

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
                INSERT IGNORE INTO station_product
                VALUES (%s, %s)
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
                for transport_device in zoom['transport_devices']:
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
        lock.acquire()

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

        for departure in departures:
            if departure.product != 'UBAHN' and departure.product != 'BUS':
                continue

            try:
                cursor.execute(sql_insert_schedule, (station_id,
                                                     departure.mvv_station_id,
                                                     departure.destination,
                                                     departure.destination,
                                                     departure.destination,
                                                     departure.departure_time,
                                                     departure.product,
                                                     departure.label))
                if cursor.rowcount == 1:
                    counter.increment()
            except pymysql.err.IntegrityError as e:
                pass
            except Exception as e:
                obj = {
                    'status': 'ERROR',
                    'module': 'schedule',
                    'message': str(e),
                    'departure': {
                        'station': name,
                        'station_id': station_id,
                        'destination': departure.destination,
                        'departure_time': departure.departure_time,
                        'product': departure.product,
                        'label': departure.label
                    }
                }
                print(json.dumps(obj))

        lock.release()

    # Every 20 minutes
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
                executor.submit(load_schedule, lock, cursor, inserted, row['station_id'], row['name'])

            executor.shutdown(wait=True)

        self.connection.commit()

        return inserted.value

    def load_depature(self, lock, cursor, counter, station_id, name):
        print('departure: ' + name)

        departures = self.mvgapi.get_departures(station_id)
        lock.acquire()

        sql_insert_departure = """
            INSERT INTO departure
            VALUES (%s, (SELECT station_id
                         FROM station
                         WHERE name = %s
                         LIMIT 1),
                    %s, %s, %s, %s, %s, %s)
        """

        for departure in departures:
            if departure.product != 'UBAHN' and departure.product != 'BUS':
                continue

            try:
                cursor.execute(sql_insert_departure, (station_id,
                                                      departure.destination,
                                                      departure.departure_time,
                                                      departure.product,
                                                      departure.label,
                                                      departure.live,
                                                      departure.sev,
                                                      departure.line_background_color))
                if cursor.rowcount == 1:
                    counter.increment()
            except pymysql.err.IntegrityError as e:
                pass
            except Exception as e:
                obj = {
                    'status': 'ERROR',
                    'module': 'departure',
                    'message': str(e),
                    'departure': {
                        'station': name,
                        'station_id': station_id,
                        'destination': departure.destination,
                        'departure_time': departure.departure_time,
                        'product': departure.product,
                        'label': departure.label,
                        'live': departure.live,
                        'sev': departure.sev,
                        'lineBackgroundColor': departure.line_background_color
                    }
                }
                print(json.dumps(obj))

        lock.release()

        # Every 5 minutes
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
                executor.submit(load_depature, lock, cursor, inserted, row['station_id'], row['name'])

            executor.shutdown(wait=True)

        self.connection.commit()

        return inserted.value

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--refresh-stations', dest='refresh_stations', action='store_true')
    group.add_argument('-z', '--refresh-zoom', dest='refresh_zoom', action='store_true')
    group.add_argument('-s', '--refresh-schedule', dest='refresh_schedule', action='store_true')
    group.add_argument('-d', '--refresh-departures', dest='refresh_departures', action='store_true')
    args = parser.parse_args()

    observer = Observer()
    observer.connect()

    if args.refresh_stations:
        print('Refreshing stations...')
        inserted, changed = observer.refresh_stations()
        print('stations inserted: ' + str(inserted))
        print('stations changed: ' + str(changed))
    elif args.refresh_zoom:
        print('Refreshing zoom data...')
        inserted = observer.refresh_zoom_data()
        print('zoom devices inserted: ' + str(inserted))
    elif args.refresh_schedule:
        print('Loading schedule...')
        inserted = observer.load_schedule_threaded()
        print('schedule items inserted: ' + str(inserted))
    elif args.refresh_departures:
        print('Loading departures...')
        inserted = observer.load_departures_threaded()
        print('departure items inserted: ' + str(inserted))

    observer.disconnect()

if __name__ == "__main__":
    main()