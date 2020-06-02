#!/home/pi/ctt/.envs/station/bin/python3 
import requests
import subprocess
import datetime
class StationUpdater:
    def __init__(self):
        self.station_id = self.getStationId()
        self.update_endpoint = 'https://station.internetofwildlife.com/station/v1/update'
        self.tmp_file = '/tmp/station-update.sh'
        self.update_log = '/data/update-{}.log'.format(self.station_id)

    def getStationId(self):
        with open('/etc/ctt/station-id', 'r') as inFile:
            return inFile.read().strip()

    def update(self):
        response = requests.post(self.update_endpoint, json={'id': self.station_id})
        if response.status_code == 200:
            print(response.text)
            with open(self.tmp_file, 'w') as outFile:
                outFile.write(response.text)
            with open(self.update_log, 'a') as outFile:
                # run bash script
                now = datetime.datetime.utcnow()
                outFile.write('{} - update\n'.format(now.strftime('%Y-%m-%d %H:%M:%S')))
                subprocess.Popen(['bash', self.tmp_file], stdout=outFile)

if __name__ == '__main__':
    updater = StationUpdater()
    updater.update()

    