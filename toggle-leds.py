import datetime
import requests
import dateutil.parser
from pytz import utc

class StationLeds:
    def __init__(self):
        self.base_endpoint = 'http://localhost:3000'

        self.gps_endpoint = '{}/gps'.format(self.base_endpoint)
        self.led_endpoint = '{}/led'.format(self.base_endpoint)
        self.leds = ['a', 'b', 'gps']

    def get(self, url):
        try:
            res = requests.get(url)
            return res.json()
        except Exception as err:
            print('error getting url', url)
            return
    def post(self, url, payload):
        try:
            kwargs = {}
            if payload is not None:
                kwargs['json'] = payload
            print('posting payload', payload, 'to', url)
            res = requests.post(url, **kwargs)
            return res.json()
        except Exception as err:
            print('error posting payload to endpoint', url, payload)
            return

    def solidLed(self, which_led, state):
        if which_led not in self.leds:
            print('invalid led', which_led)
            return
        endpoint = self.led_endpoint
        if which_led != 'gps':
            endpoint = '{}/{}'.format(self.led_endpoint, 'diag')
        endpoint = '{}/{}'.format(endpoint, which_led)
        payload = {
            'state': state
        }
        self.post(endpoint, payload=payload)

    def blinkLed(self, which_led, rate):
        if which_led not in self.leds:
            print('invalid led', which_led)
            return
        endpoint = self.led_endpoint
        if which_led != 'gps':
            endpoint = '{}/{}'.format(self.led_endpoint, 'diag')
        endpoint = '{}/{}'.format(endpoint, which_led)
        payload = {
            'state': 'blink',
            'blink_ms': rate
        }
        self.post(endpoint, payload=payload)
        
    def toggleGpsLed(self):
        try:
            res = requests.get(self.gps_endpoint)
        except Exception as err:
            print("error toggling GPS led")
            print(err)
            return

        if res.status_code == 200:
            response = res.json()
            info = response['gps']
            mode = info.get('mode')
            print('toggling GPS LED', mode)
            if mode == 3:
                self.solidLed(which_led='gps', state='on')
            elif mode == 2:
                self.blinkLed(which_led='gps', rate=500)
            elif mode == 1:
                self.blinkLed(which_led='gps', rate=200)
            else:
                self.solidLed(which_led='gps', state='off')
        
    def checkInternetStatus(self, ping_count):
        url = '{}/internet/status?ping_count={}'.format(
            self.base_endpoint, 
            ping_count
        )

        try:
            res = requests.get(url)
        except Exception as err:
            logging.error(err)
            return False

        if (res.status_code == 200):
            response = res.json()
            if response['success'] == ping_count:
                return True
        return False

    def toggleModemLight(self):
        if self.checkInternetStatus(ping_count=3) is True:
            self.solidLed(which_led='b', state='on')
        else:
            self.solidLed(which_led='b', state='off')

    def checkRadioServer(self):
        now = datetime.datetime.utcnow().replace(tzinfo=utc)
        url = '{}/radio/stats'.format(self.base_endpoint)
        res = self.get(url)
        stat_time = dateutil.parser.parse(res['now'])
        delta = (now-stat_time).seconds
        print(delta)
        if delta > 30:
            self.blinkLed(which_led='a', rate=100)
        else:
            self.solidLed(which_led='a', state='on')


if __name__ == '__main__':
    leds = StationLeds()
    #leds.toggleGpsLed()
    #leds.toggleModemLight()
    leds.checkRadioServer()