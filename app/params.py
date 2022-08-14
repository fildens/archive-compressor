import json
import logging
import os
import ssl
import sys
from multiprocessing import Queue
from pathlib import Path

import requests
from decouple import UndefinedValueError, AutoConfig
from peewee import *
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager


class Tls12HttpAdapter(HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_version=ssl.PROTOCOL_TLSv1)


class Conf:
    def __init__(self):
        self.ScriptPath = Path(__file__).parent.parent.absolute()
        self.ScriptName = ''
        try:
            params = json.load(Path(self.ScriptPath, 'configuration.json').open('r'))
        except (FileNotFoundError, KeyError, json.decoder.JSONDecodeError):
            logging.error('Configuration file is broken or wrong path')
            sys.exit(1)
        else:
            env_config = AutoConfig(search_path=Path(self.ScriptPath, '.env'))
            try:
                # from local .env file
                self.db_name = env_config('DB_NAME')
                self.db_host = env_config('DB_HOST')
                self.db_user = env_config('DB_USER')
                self.db_pass = env_config('DB_PASS')
                self.es_user = env_config('ES_USER')
                self.es_pass = env_config('ES_PASS')
                self.log_to_file = int(env_config('LOG'))
            except UndefinedValueError:
                # from env variables
                self.db_name = os.getenv('DB_NAME')
                self.db_host = os.getenv('DB_HOST')
                self.db_user = os.getenv('DB_USER')
                self.db_pass = os.getenv('DB_PASS')
                self.es_user = os.getenv('ES_USER')
                self.es_pass = os.getenv('ES_PASS')
                self.log_to_file = 0

            self.auth = (self.es_user, self.es_pass)

            self.LimitFiles = params['limit_files_number']
            self.MS = params['work_media_space']
            self.AME_SRV = params['adobe_media_encoder_host']
            self.TempFolder = params['temp_folder_name']
            self.RAIDIX = params['raidix_ip']
            self.FilesLocation = params['work_files_location']
            self.ff_path = params['path_to_ffmpeg']
            self.srv_transfer = params['transfer_server_host']
            self.srv_scan = params['scan_server_host']
            self.srv_api = params['api_server_host']
            self.os_win = params['host_operation_system_win']
            self.ScriptPath = Path(__file__).parent.parent.absolute()

            self.arc_compress = MySQLDatabase(
                self.db_name,
                user=self.db_user,
                password=self.db_pass,
                host=self.db_host,
                charset='utf8',
                autoconnect=False,
                thread_safe=True)

            self.stop_service = 0
            self.Q = Queue()
            self.ItemLength = 0
            self.PathData = list()
            self.ProblemList = list()

            # self.MediaLocationWin = Path(r'\\{}\{}'.format(self.RAIDIX, self.MS))
            self.MediaLocationWin = Path(self.FilesLocation)
            self.MediaLocationLin = self.ScriptPath.joinpath(self.MS)
            self.MediaLocation = self.MediaLocationWin if self.os_win else self.MediaLocationLin

            self.EPR = Path(r'\\{}\{}'.format(self.RAIDIX, 'ARC.epr'))

            # transfer (upload, download, delete, copy, move)
            self.s_transfer = requests.session()
            self.s_transfer.auth = self.auth
            self.s_transfer.verify = False
            self.s_transfer.mount(self.srv_transfer, Tls12HttpAdapter())

            # scan only
            self.s_scan = requests.session()
            self.s_scan.auth = self.auth
            self.s_scan.verify = False
            self.s_scan.mount(self.srv_scan, Tls12HttpAdapter())

            # search, files, clips, meta, sequence, project, users
            self.s_api = requests.session()
            self.s_api.auth = self.auth
            self.s_api.verify = False
            self.s_api.mount(self.srv_api, Tls12HttpAdapter())

            self.HELPER_MODE = True
            self.AME = False
