import requests
import time
from datetime import datetime
from xml.etree import ElementTree

DEFAULT_BASE_URL = 'https://mobile.defas-fgi.de/xml/'


class UnexpectedResponseCodeException(Exception):

    def __init__(self, code, message=None, *args):
        self.message = 'Server responded with {}'.format(code)
        self.code = code

        super(UnexpectedResponseCodeException,
              self).__init__(message, *args)


class Departure(object):

    def __init__(self, mvv_station_id = None, station_name = None, departure_time = None,
                 product = None, label = None, destination = None):
        self._mvv_station_id = mvv_station_id
        self._station_name = station_name
        self._departure_time = departure_time
        self._product = product
        self._label = label
        self._destination = destination

    @property
    def mvv_station_id(self):
        return self._mvv_station_id

    @property
    def station_name(self):
        return self._station_name

    @property
    def departure_time(self):
        return self._departure_time

    @property
    def product(self):
        return self._product

    @property
    def label(self):
        return self._label

    @property
    def destination(self):
        return self._destination


class MVVAPI(object):

    def __init__(self, base_url=DEFAULT_BASE_URL, user_agent='Dalvik/2.1.0 (Linux; U; Android 7.0;'):
        self._base_url = base_url
        self._user_agent = user_agent

    def _generate_headers(self):
        return {
            'User-Agent': self._user_agent,
        }

    def _authenticated_request(self, method, endpoint, params=None, data=None):
        response = requests.request(method=method, url=self._base_url + endpoint,
                                    headers=self._generate_headers(), params=params, data=data)

        if response.status_code == requests.codes.ok:
            return response
        else:
            raise UnexpectedResponseCodeException(response.status_code)

    def get_departures(self, station, zug=False, sbahn=False, ubahn=True, tram=False, bus=True,
                       icbus=False, expressbus=False, limit=5):
        params = {
            'mode': 'direct',
            'useRealtime': 0,
            'name_dm': station,
            'type_dm': 'stop',
            'limit': limit,
            'excludedMeans': 'checkbox'
        }

        if not zug:
            params['exclMOT_0'] = 1
        if not sbahn:
            params['exclMOT_1'] = 1
        if not ubahn:
            params['exclMOT_2'] = 1
        if not tram:
            params['exclMOT_4'] = 1
        if not bus:
            params['exclMOT_5'] = 1
        if not icbus:
            params['exclMOT_6'] = 1
        if not expressbus:
            params['exclMOT_7'] = 1

        response = self._authenticated_request('GET', 'XML_DM_REQUEST', params)

        departures = []
        dps_xml = ElementTree.fromstring(response.content).find('dps')
        if dps_xml:
            departures_xml = dps_xml.findall('dp')
            for departure_xml in departures_xml:
                departure = Departure()
                for node in departure_xml:
                    if node.tag == 'n':
                        departure._station_name = node.text
                    elif node.tag == 'st':
                        date = '{} {}'.format(node.find('da').text, node.find('t').text)
                        timestamp = time.mktime(datetime.strptime(date, "%Y%m%d %H%M").timetuple())
                        departure._departure_time = int(timestamp)
                    elif node.tag == 'm':
                        product = node.find('n').text.upper().replace('-', '')
                        departure._product = product
                        departure._label = node.find('nu').text
                        departure._destination = node.find('des').text
                    elif node.tag == 'r':
                        departure._mvv_station_id = int(node.find('id').text)

                departures.append(departure)

        return departures
