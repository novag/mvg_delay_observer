# =====================================================
# Copyright (c) 2017 novag All Rights Reserved.
# 
# Confidential and Proprietary - novag
# =====================================================

import json
import mvg_api
import mvv_api
import os
import pymysql
import time

connection = pymysql.connect(host=os.environ['HOST'],
                             user=os.environ['USER'],
                             password=os.environ['PASSWORD'],
                             db=os.environ['DB'],
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
mvgapi = mvg_api.MVGAPI()
mvvapi = mvv_api.MVVAPI()

def refresh_station(stations = None):
    inserted = 0
    changed = 0

    if not stations:
        stations = mvgapi.get_stations().stations
    with connection.cursor() as cursor:
        for station in stations:
            sql = "INSERT INTO station (station_id, type, name, aliases, hasLiveData, hasZoomData, place, longitude, latitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE station_id=%s, type=%s, name=%s, aliases=%s, hasLiveData=%s, hasZoomData=%s, place=%s, longitude=%s, latitude=%s"
            cursor.execute(sql, (station.id, station.type, station.name, station.aliases, station.has_live_data, station.has_zoom_data, station.place, station.longitude, station.latitude,
                                 station.id, station.type, station.name, station.aliases, station.has_live_data, station.has_zoom_data, station.place, station.longitude, station.latitude))

            if cursor.rowcount == 1:
                inserted += 1
            elif cursor.rowcount == 2:
                changed += 1

            for product in station.products:
                sql = "INSERT IGNORE INTO station_product VALUES (%s, %s)"
                cursor.execute(sql, (station.id, product))

    connection.commit()

    return inserted, changed

def refresh_zoom_data():
    inserted = 0

    with connection.cursor() as cursor:
        sql = "SELECT station_id FROM station WHERE hasZoomData = 1"
        cursor.execute(sql)
        stations = cursor.fetchall()
        for row in stations:
            zoom = mvgapi.get_zoom_data(row['station_id'])
            for transport_device in zoom['transport_devices']:
                sql = "SELECT status FROM transport_device WHERE station_id = %s AND xcoordinate = %s AND ycoordinate = %s ORDER BY timestamp DESC LIMIT 1"
                cursor.execute(sql, (row['station_id'], transport_device.xcoordinate, transport_device.ycoordinate))
                latest_entry = cursor.fetchone()

                if not latest_entry or latest_entry['status'] != transport_device.status:
                    sql = "INSERT INTO transport_device (station_id, type, identifier, name, description, status, oos_since, oos_until, oos_description, timestamp, xcoordinate, ycoordinate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (zoom.station_id, transport_device.type, transport_device.identifier, transport_device.name, transport_device.description, transport_device.status, transport_device.oos_since, transport_device.oos_until, transport_device.oos_description, transport_device.last_update, transport_device.xcoordinate, transport_device.ycoordinate))
                    inserted += 1
            time.sleep(0.3)

    connection.commit()

    return inserted

def load_schedule(cursor, station):
    inserted = 0

    sql = "SELECT station_id FROM station WHERE name = %s"
    cursor.execute(sql, (station))
    station_id = cursor.fetchone()['station_id']

    departures = mvvapi.get_departures(station)
    for departure in departures:
        sql = """INSERT INTO schedule
                 VALUES (%s, %s, (SELECT station_id
                                  FROM station
                                  WHERE MATCH (name) AGAINST (%s IN NATURAL LANGUAGE MODE)
                                  ORDER BY (CASE WHEN
                                                name = %s THEN 1 ELSE 0
                                            END) DESC,
                                           (MATCH (name) AGAINST (%s IN NATURAL LANGUAGE MODE)) DESC
                                  LIMIT 1),
                         %s, %s, %s)"""
        try:
            cursor.execute(sql, (station_id, departure.mvv_station_id, departure.destination, departure.destination, departure.destination, departure.departure_time, departure.product, departure.label))
            if cursor.rowcount == 1:
                inserted += 1
        except pymysql.err.IntegrityError as e:
            pass
        except pymysql.err.InternalError as e:
            obj = {
                'status': 'ERROR',
                'module': 'schedule',
                'message': str(e),
                'departure': {
                    'station': station,
                    'station_id': station_id,
                    'destination': departure.destination,
                    'departure_time': departure.departure_time,
                    'product': departure.product,
                    'label': departure.label
                }
            }
            print(json.dumps(obj))

    return inserted


#inserted, changed = refresh_stations()
#print('stations inserted: ' + str(inserted))
#print('stations changed: ' + str(changed))

#inserted = refresh_zoom_data()
#print('transport devices inserted: ' + str(inserted))

#stations = mvgapi.get_stations().stations
inserted = 0
with connection.cursor() as cursor:
    #for station in stations:
    #    inserted += load_schedule(cursor, 'Alte Heide')
    inserted += load_schedule(cursor, 'Alte Heide')

print('planned departures inserted: ' + str(inserted))

connection.commit()

connection.close()