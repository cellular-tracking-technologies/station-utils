#!/home/pi/ctt/.envs/station/bin/python3 
import datetime
import time
import json
import glob
import os
import logging
import shutil
import requests

logging.basicConfig(level=logging.INFO)

class StationUploader:
    def __init__(self):
        self.endpoint = "http://station.internetofwildlife.com/station/v2/upload"
        self.sg_file_dir = os.path.join('/', 'data', 'SGdata')
        self.rotated_dir = os.path.join('/', 'data', 'rotated')
        self.base_uploaded_dir = os.path.join('/', 'data', 'uploaded')
        self.ctt_uploaded_dir = os.path.join(self.base_uploaded_dir, 'ctt')
        self.sg_uploaded_dir = os.path.join(self.base_uploaded_dir, 'sg')
        self.hardware_server_port = 3000
        self.internet_check_ping_count = 3
        self.ensureDirs()
        self.station_id = self.getStationId()

    def getStationId(self):
        with open('/etc/ctt/station-id', 'r') as inFile:
            return inFile.read().strip()

    def ensureDirs(self):
        os.makedirs(self.ctt_uploaded_dir, exist_ok=True)
        os.makedirs(self.sg_uploaded_dir, exist_ok=True)

    def checkInternetStatus(self):
        url = 'http://localhost:{}/internet/status?ping_count={}'.format(
            self.hardware_server_port, 
            self.internet_check_ping_count
        )

        try:
            res = requests.get(url)
        except Exception as err:
            logging.error(err)
            return False

        if (res.status_code == 200):
            response = res.json()
            if response['success'] == self.internet_check_ping_count:
                return True
        return False

    def uploadFile(self, fileuri, filetype):
        endpoint = self.endpoint
        if filetype == 'sg':
            endpoint = '{}/sg'.format(endpoint)
        else:
            endpoint = '{}/ctt'.format(endpoint)
        with open(fileuri, 'rb') as inFile:
            contents = inFile.read()
            headers = {
                'filename': os.path.basename(fileuri),
                'Content-Type': 'application/octet-stream'
            }
            response = requests.post(endpoint, headers=headers, data=contents)
            # check for a 204 response code for validation
            if response.status_code == 204:
                return True
            else:
                print('invalid server response code {}'.format(response.status_code))
        return False

    def rotateUploaded(self, fileuri, filetype):
        basename = os.path.basename(fileuri)
        if filetype == 'sg':
            uploaded_dir = self.sg_uploaded_dir
        else:
            uploaded_dir = self.ctt_uploaded_dir
        now = datetime.datetime.utcnow()
        uploaded_dir = os.path.join(uploaded_dir, now.strftime('%Y-%m-%d'))
        os.makedirs(uploaded_dir, exist_ok=True)
        newuri = os.path.join(uploaded_dir, basename)
        print('moving file', os.path.basename(fileuri), 'to', newuri)
        shutil.move(fileuri, newuri)

    def uploadAllCttFiles(self):
        filenames = glob.glob(os.path.join(self.rotated_dir, '*'))
        logging.info('about to upload {} CTT data files'.format(len(filenames)))
        if self.checkInternetStatus() is True:
            for filename in sorted(filenames):
                res = self.uploadFile(fileuri=filename, filetype='ctt')
                if res is False:
                    # if we cannot upload a file - don't upload the rest
                    print('problem uploading these files - stopping upload process')
                    return False
                self.rotateUploaded(fileuri=filename, filetype='ctt')
            return True
        else:
            print('no internet connection - not uploading anything')
        return False

    def uploadAllSgFiles(self):
        filenames = glob.glob(os.path.join(self.sg_file_dir, '*', '*.gz'))
        logging.info('about to upload {} SG files'.format(len(filenames)))
        now = datetime.datetime.utcnow()
        if self.checkInternetStatus() is True:
            for filename in sorted(filenames):

                delta = (time.time() - os.stat(filename).st_mtime) / 60.0 # minutes since last modified
                if delta  > 61:
                    # upload files older than 1 hour
                    res = self.uploadFile(fileuri=filename, filetype='sg')
                    if res is False:
                        print('problem uploading files - aborting upload')
                        return false
                    self.rotateUploaded(fileuri=filename, filetype='sg')
            return True
        else:
            print('no internet connection - not uploading anything')
        return False

if __name__ == '__main__':
    uploader = StationUploader()
    res = uploader.uploadAllCttFiles()
    if res is True:
        uploader.uploadAllSgFiles()
