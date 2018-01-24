import requests

DEFAULT_BASE_URL = 'https://apps.mvg-fahrinfo.de/v10/rest/10.0/'
DEFAULT_API_KEY = 'aklKa290LsadOLW'


class UnexpectedResponseCodeException(Exception):

    def __init__(self, code, message=None, *args):
        self.message = 'Server responded with {}'.format(code)
        self.code = code

        super(UnexpectedResponseCodeException,
              self).__init__(message, *args)


class Station(object):

    def __init__(self, type, latitude, longitude, id, place, name, has_live_data, has_zoom_data,
                 products, aliases, link=None):
        self._type = type
        self._latitude = latitude
        self._longitude = longitude
        self._id = id
        self._place = place
        self._name = name
        self._has_live_data = has_live_data
        self._has_zoom_data = has_zoom_data
        self._products = products
        self._aliases = aliases
        self._link = link

    @property
    def type(self):
        return self._type

    @property
    def latitude(self):
        return self._latitude

    @property
    def longitude(self):
        return self._longitude

    @property
    def id(self):
        return self._id

    @property
    def place(self):
        return self._place

    @property
    def name(self):
        return self._name

    @property
    def has_live_data(self):
        return self._has_live_data

    @property
    def has_zoom_data(self):
        return self._has_zoom_data

    @property
    def products(self):
        return self._products

    @property
    def aliases(self):
        return self._aliases

    @property
    def link(self):
        return self._link


class StationResponse(object):

    def __init__(self, hash, version, stations):
        self._hash = hash
        self._version = version
        self._stations = stations

    @property
    def hash(self):
        return self._hash

    @property
    def version(self):
        return self._version

    @property
    def stations(self):
        return self._stations


class TransportDevice(object):

    def __init__(self, status, name, identifier, xcoordinate, ycoordinate, description, type,
                 last_update, oos_since=None, oos_until=None, oos_description=None):
        self._status = status
        self._name = name
        self._identifier = identifier
        self._xcoordinate = xcoordinate
        self._ycoordinate = ycoordinate
        self._description = description
        self._type = type
        self._last_update = last_update
        self._oos_since = oos_since
        self._oos_until = oos_until
        self._oos_description = oos_description

    @property
    def status(self):
        return self._status

    @property
    def name(self):
        return self._name

    @property
    def identifier(self):
        return self._identifier

    @property
    def xcoordinate(self):
        return self._xcoordinate

    @property
    def ycoordinate(self):
        return self._ycoordinate

    @property
    def description(self):
        return self._description

    @property
    def type(self):
        return self._type

    @property
    def last_update(self):
        return self._last_update

    @property
    def oos_since(self):
        return self._oos_since

    @property
    def oos_until(self):
        return self._oos_until

    @property
    def oos_description(self):
        return self._oos_description


class ZoomResponse(object):

    def __init__(self, station_id, name, transport_devices):
        self._station_id = station_id
        self._name = name
        self._transport_devices = transport_devices

    @property
    def station_id(self):
        return self._station_id

    @property
    def name(self):
        return self._name

    @property
    def transport_devices(self):
        return self._transport_devices


class Departure(object):

    def __init__(self, departure_time, product, label, destination, live, line_background_color,
                 departure_id, sev):
        self._departure_time = departure_time
        self._product = product
        self._label = destination
        self._destination = destination
        self._live = live
        self._line_background_color = line_background_color
        self._departure_id = departure_id
        self._sev = sev

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

    @property
    def live(self):
        return self._live

    @property
    def line_background_color(self):
        return self._line_background_color

    @property
    def departure_id(self):
        return self._departure_id

    @property
    def sev(self):
        return self._sev


class MVGAPI(object):

    def __init__(self, base_url=DEFAULT_BASE_URL, api_key=DEFAULT_API_KEY, user_agent='MVG Fahrinfo Android 5.4'):
        self._base_url = base_url
        self._api_key = api_key
        self._user_agent = user_agent

    def _generate_headers(self):
        return {
            'User-Agent': self._user_agent,
            'Accept': 'application/json'
        }

    def _authenticated_request(self, method, endpoint, params={}, data=None):
        params['apiKey'] = self._api_key
        response = requests.request(method=method, url=self._base_url + endpoint,
                                    headers=self._generate_headers(), params=params, data=data)

        if response.status_code == requests.codes.ok:
            return response
        else:
            raise UnexpectedResponseCodeException(response.status_code)

    def _get_stations(self, data_hash='64bfd16917ce3fbc5585bc14e4dee26a', version=0):
        params = {
            'hash': data_hash,
            'version': version
        }

        return self._authenticated_request('GET', 'dynamicdata/stationData', params)

    def get_stations(self, data_hash='64bfd16917ce3fbc5585bc14e4dee26a', version=0):
        response = self._get_stations(data_hash, version)

        stations = []
        for station in response.json()['stations']:
            stations.append(Station(station['type'], station['latitude'], station['longitude'],
                                    station['id'], station['place'], station['name'],
                                    station['hasLiveData'], station['hasZoomData'],
                                    station['products'], station['aliases']))

        return StationResponse(response.json()['hash'], response.json()['version'], stations)

    def _get_zoom_data(self, station_id):
        response = self._authenticated_request('GET', 'zoom/{}'.format(station_id))

        return response

    def get_zoom_data(self, station_id):
        response = self._get_zoom_data(station_id)

        transport_devices = []
        for transport_device in response.json()['transportDevices']:
            if 'lastUpdate' not in transport_device:
                transport_device['lastUpdate'] = -1

            if 'planned' in transport_device:
                transport_devices.append(TransportDevice(transport_device['status'],
                                                         transport_device['name'],
                                                         transport_device['identifier'],
                                                         transport_device['xcoordinate'],
                                                         transport_device['ycoordinate'],
                                                         transport_device['description'],
                                                         transport_device['type'],
                                                         transport_device['lastUpdate'],
                                                         transport_device['planned']['since'],
                                                         transport_device['planned']['until'],
                                                         transport_device['planned']['description']))
            else:
                transport_devices.append(TransportDevice(transport_device['status'],
                                                         transport_device['name'],
                                                         transport_device['identifier'],
                                                         transport_device['xcoordinate'],
                                                         transport_device['ycoordinate'],
                                                         transport_device['description'],
                                                         transport_device['type'],
                                                         transport_device['lastUpdate']))

        return ZoomResponse(response.json()['efaId'], response.json()['name'], transport_devices)

    def _get_departures(self, station_id, ubahn=True, sbahn=False, tram=False, bus=True, zug=False):
        params = {
            'ubahn': ubahn,
            'sbahn': sbahn,
            'tram': tram,
            'bus': bus,
            'zug': zug
        }

        return self._authenticated_request('GET', 'departure/{}'.format(station_id), params)

    def get_departures(self, station_id, ubahn=True, sbahn=False, tram=False, bus=True, zug=False, regiobus=False):
        response = self._get_departures(station_id, ubahn, sbahn, tram, bus, zug)

        departures = []
        for departure in response.json()['departures']:
            if not regiobus and departure['product'] == 'REGIONAL_BUS':
                continue

            departures.append(Departure(departure['departureTime'], departure['product'],
                                        departure['label'], departure['destination'],
                                        departure['live'], departure['lineBackgroundColor'],
                                        departure['departureId'], departure['sev']))

        return departures

    def get_departures_list(self, station_id, ubahn=True, sbahn=False, tram=False, bus=True, zug=False, regiobus=False):
        response = self._get_departures(station_id, ubahn, sbahn, tram, bus, zug)

        departures = []
        for departure in response.json()['departures']:
            if not regiobus and departure['product'] == 'REGIONAL_BUS':
                continue

            departures.append((station_id,
                               departure['destination'],
                               departure['departureId'],
                               departure['departureTime'],
                               departure['product'],
                               departure['label'],
                               departure['live'],
                               departure['sev'],
                               departure['lineBackgroundColor']))

        return departures
