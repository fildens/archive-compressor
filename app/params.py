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
from requests.adapters import HTTPAdapter, PoolManager


class TlsAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, block=False):
        self.poolmanager = PoolManager(*args, block=block, ssl_version=ssl.PROTOCOL_TLSv1)


class Conf:
    def __init__(self):
        self.ScriptPath = Path(__file__).parent.parent.absolute()
        self.ScriptName = ''
        try:
            params = json.load(Path(self.ScriptPath, 'configuration.json').open('r'))
        except (FileNotFoundError, KeyError, json.decoder.JSONDecodeError):
            logging.error('Configuration file is broken or wrong path')
            params = {
                "adobe_media_encoder_host": "http://192.168.10.11:8080",
                "path_to_ffmpeg": "/home/liveu/ffmpeg_nvidia/ffmpeg",
                "path_to_ffprobe": "/home/liveu/ffmpeg_nvidia/ffprobe",
                "transfer_server_host": "https://10.2.0.26:12194",
                "scan_server_host": "https://10.2.0.30:12134",
                "api_server_host": "https://10.2.0.20:12154",
                "limit_files_number": 500,
                "work_media_space": "ARCHIVE",
                "work_files_location": "/mnt/ARCHIVE",
                "temp_folder_name": "TEMP_TRANSFER"
            }
            with Path(self.ScriptPath, 'configuration.json').open('w', encoding='utf-8') as f:
                json.dump(params, f, ensure_ascii=False, sort_keys=True, indent=4)
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
            if not self.db_host:
                logging.error('No .env file found and no VENV translated to script !!!')
                sys.exit(1)
            self.auth = (self.es_user, self.es_pass)

            self.LimitFiles = params['limit_files_number']
            self.MS = params['work_media_space']
            self.AME_SRV = params['adobe_media_encoder_host']
            self.TempFolder = params['temp_folder_name']
            self.FilesLocation = params['work_files_location']
            self.ffm_path = params['path_to_ffmpeg']
            self.ffp_path = params['path_to_ffprobe']
            self.srv_transfer = params['transfer_server_host']
            self.srv_scan = params['scan_server_host']
            self.srv_api = params['api_server_host']
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

            self.MediaLocation = Path(self.FilesLocation)

            self.EPR = Path(self.ScriptPath, 'ARC.epr')

            # transfer (upload, download, delete, copy, move)
            self.s_transfer = requests.session()
            self.s_transfer.auth = self.auth
            self.s_transfer.verify = False
            self.s_transfer.mount(self.srv_transfer, TlsAdapter())

            # scan only
            self.s_scan = requests.session()
            self.s_scan.auth = self.auth
            self.s_scan.verify = False
            self.s_scan.mount(self.srv_scan, TlsAdapter())

            # search, files, clips, meta, sequence, project, users
            self.s_api = requests.session()
            self.s_api.auth = self.auth
            self.s_api.verify = False
            self.s_api.mount(self.srv_api, TlsAdapter())

            self.HELPER_MODE = True
            self.AME = False
