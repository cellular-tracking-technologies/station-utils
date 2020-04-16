#!/home/pi/ctt/.envs/station/bin/python3 
import boto3
import json
import glob
import os
import logging
import shutil
import requests
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

class StationUploader:
    def __init__(self):
        self.bucket = 'ctt-motus-development'
        self.base_key = 'ctt/tag-data-v2'
        self.rotated_dir = os.path.join('/', 'data', 'rotated')
        self.base_uploaded_dir = os.path.join('/', 'data', 'uploaded')
        self.ctt_uploaded_dir = os.path.join(self.base_uploaded_dir, 'ctt')
        self.hardware_server_port = 3000
        self.internet_check_ping_count = 3
        self.ensureDirs()
        with open ('/etc/ctt/ctt.conf', 'r') as inFile:
            data = json.loads(inFile.read())
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=data['accessKeyId'],
                aws_secret_access_key=data['secretAccessKey']
            )

    def ensureDirs(self):
        os.makedirs(self.ctt_uploaded_dir, exist_ok=True)

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

    def uploadFile(self, fileuri, key):
        print('about to upload file', fileuri)
        try:
            response = self.s3.upload_file(fileuri, self.bucket, key)
        except Exception as err:
            logging.error(err)
            return False
        return True

    def uploadCttFile(self, fileuri):
        basename = os.path.basename(fileuri)
        key = '{}/{}'.format(self.base_key, basename)
        return self.uploadFile(fileuri, key)

    def rotateUploaded(self, fileuri):
        basename = os.path.basename(fileuri)
        newuri = os.path.join(self.ctt_uploaded_dir, basename)
        print('moving file', fileuri, 'to', newuri)
        shutil.move(fileuri, newuri)

    def uploadAllCttFiles(self):
        logging.info('about to upload CTT data files')
        filenames = glob.glob(os.path.join(self.rotated_dir, '*'))
        if self.checkInternetStatus() is True:
            for filename in filenames:
                res = self.uploadCttFile(filename)
                if res is False:
                    # if we cannot upload a file - don't upload the rest
                    print('problem uploading these files - stopping upload process')
                    return False
                self.rotateUploaded(filename)
        else:
            print('no internet connection - not uploading anything')
        return True

if __name__ == '__main__':
    uploader = StationUploader()
    uploader.uploadAllCttFiles()
