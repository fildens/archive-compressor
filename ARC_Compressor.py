# -*- coding: cp1251 -*-

import argparse
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from multiprocessing import Queue
from os.path import relpath
from pathlib import Path
from threading import Thread

import requests
import urllib3
from peewee import *
from requests.adapters import HTTPAdapter

from lib.logger import Logger

from decouple import config, UndefinedValueError

# init environment variables
try:
    # from local .env file
    db_name = config('DB_NAME')
    db_host = config('DB_HOST')
    db_user = config('DB_USER')
    db_pass = config('DB_PASS')
    es_user = config('ES_USER')
    es_pass = config('ES_PASS')
    log_to_file = int(config('LOG'))
except UndefinedValueError:
    # from env variables
    db_name = os.getenv('DB_NAME')
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    es_user = config('ES_USER')
    es_pass = config('ES_PASS')
    log_to_file = 0

ScriptName = Path(__file__).stem
ScriptPath = Path(__file__).parent.absolute()

L = Logger(ScriptName, ScriptPath, sql=False, err_log=True)
logger = L.build()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ap = argparse.ArgumentParser()

try:
    params = json.load(Path('configuration.json').open('r'))
except (FileNotFoundError, KeyError, json.decoder.JSONDecodeError):
    logging.error('Configuration file is broken or wrong path')
    sys.exit(1)
else:
    LimitFiles = params['limit_files_number']
    MS = params['destination_media_space']
    AME_SRV = params['adobe_media_encoder_host']
    TempFolder = params['temp_folder_name']
    RAIDIX = params['raidix_ip']
    ff_path = params['path_to_ffmpeg']
    srv_transfer = params['transfer_server_host']
    srv_scan = params['scan_server_host']
    srv_api = params['api_server_host']
    os_win = params['host_operation_system_win']


stop_service = 0
Q = Queue()
ItemLength = 0
PathData = list()
ProblemList = list()

MediaLocationWin = Path(r'\\{}\{}'.format(RAIDIX, MS))
MediaLocationLin = ScriptPath.joinpath(MS)
MediaLocation = MediaLocationWin if os_win else MediaLocationLin

EPR = MediaLocation.joinpath('ARC.epr')

a_30 = HTTPAdapter(max_retries=30)
a_10 = HTTPAdapter(max_retries=10)
a_5 = HTTPAdapter(max_retries=5)
a_2 = HTTPAdapter(max_retries=2)

auth = (es_user, es_pass)

# transfer (upload, download, delete, copy, move)
s_transfer = requests.session()
s_transfer.auth = auth
s_transfer.verify = False
s_transfer.mount(srv_transfer, a_10)

# scan only
s_scan = requests.session()
s_scan.auth = auth
s_scan.verify = False
s_scan.mount(srv_scan, a_2)

# search, files, clips, meta, sequence, project, users
s_api = requests.session()
s_api.auth = auth
s_api.verify = False
s_api.mount(srv_api, a_2)

# db = SqliteDatabase(ScriptPath.joinpath('database.db'))
db = MySQLDatabase(
    db_name,
    user=db_user,
    password=db_pass,
    host=db_host,
    charset='utf8',
    autoconnect=False,
    thread_safe=True)


class JSONField(TextField):
    def db_value(self, value):
        return json.dumps(value, indent=4, sort_keys=True, ensure_ascii=False)

    def python_value(self, value):
        if value is not None:
            try:
                json_data = json.loads(value)
            except json.decoder.JSONDecodeError:
                return json.loads('{}')
            else:
                return json_data


class ReportBase(Model):
    in_work = BooleanField(default=False)
    transcode = BooleanField(default=False)
    clip_delete = BooleanField(default=False)
    file_copy = BooleanField(default=False)
    file_remove = BooleanField(default=False)
    scan = BooleanField(default=False)
    item = JSONField(null=False)
    duration = FloatField(default=0.0)
    orig_size = IntegerField(default=0)
    dst_size = IntegerField(default=0)
    userpath = TextField(default='')
    captured = TextField(default='')
    clip_id = IntegerField(default='')

    class Meta:
        database = db
        table_name = ''
        # legacy_table_names = False


class ErrorsBase(Model):
    item = JSONField(null=False)
    userpath = TextField(default='')
    clip_id = IntegerField(default='')
    problem = TextField(default='No')

    class Meta:
        database = db
        table_name = '!_errors_!'


class PathsBase(Model):
    old_path = TextField(default='')
    physics_path = TextField(default='')
    new_path = TextField(default='')

    class Meta:
        database = db
        table_name = '!_paths_!'


def create_sql_tables(name):
    with db:
        ReportBase._meta.table_name = name
        ReportBase.create_table()
        ErrorsBase.create_table()
        PathsBase.create_table()


def excepthook(type_, value, traceback):
    import signal
    from traceback import format_exception
    Q.put(-1)
    errmsg = ''.join(format_exception(type_, value, traceback))
    logging.error(errmsg)
    dtn = datetime.now().strftime('%d.%m.%Y_%H:%M')
    message = '{}\n--------------{}--------------\n{}\n\n'.format(dtn, ScriptName, errmsg)
    Q.put([{'END'}, 666])
    sendmail(message, 'ERROR')
    prepare_report()
    os.kill(os.getpid(), signal.SIGTERM)


def sendmail(message, subject):
    from email.mime.text import MIMEText
    import smtplib
    me = 'trouble@tv.ru'
    you = 'dfilippov@78.ru'
    # smtp_server = 'mail.78.ru'
    smtp_server = '10.78.2.247'
    msg_ = MIMEText(message)
    msg_['Subject'] = '{} {}'.format(ScriptName.upper(), subject)
    msg_['From'] = me
    msg_['To'] = you
    try:
        s = smtplib.SMTP(smtp_server, timeout=5)
        s.sendmail(me, [you], msg_.as_string())
        s.quit()
    except BaseException as e:
        logging.error('Can`t sent email: {}'.format(repr(e)))
        logging.info('\nsubject: {}\nmessage:"{}"'.format(subject, message))


def remove_wrong_paths(item):
    number_locations = len(item['data']['video'][0]['file']['locations'])
    clip_id = item['clip_id']
    if number_locations > 1:
        logging.warning('Found more then ONE LOCATIONS for filename {}'.format(item['data']['display_name']))
    userpath_real = ''
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        userpath = location['userpath']
        input_file = MediaLocation.joinpath(userpath)
        if input_file.exists():
            if input_file.stat().st_size < 10:
                logging.critical('Less 10 bytes for {} clip_id={}'.format(input_file, clip_id))
            else:
                es_orig_size = int(item['data']['video'][0]['file']['file']['filesize'])
                ch_orig_size = input_file.stat().st_size

                if abs(es_orig_size - ch_orig_size) > es_orig_size / 20:
                    logging.warning(
                        'ORIG FILE HAVE WRONG SIZE:'
                        '\nES_Size={}\nStat_Size={}\nDifference={}'.format(
                            es_orig_size,
                            ch_orig_size,
                            abs(es_orig_size - ch_orig_size)))
                item['data']['video'][0]['file']['locations'].append(location)
                userpath_real = userpath
        else:
            logging.warning('REMOVED wrong path {}'.format(location['userpath']))
    if len(item['data']['video'][0]['file']['locations']) == 0:
        return {}, '', ''
    else:
        return item, userpath_real, ''


def restore_path(item):
    from lib import LegalPath
    problem_message = 'No source file for all locations'
    number_locations = len(item['data']['video'][0]['file']['locations'])
    file_size = item['data']['video'][0]['file']['file']['filesize']
    list_userpaths = list()
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        list_userpaths.append(location['userpath'])
        input_file = MediaLocation.joinpath(location['userpath'])
        if os.path.split(input_file.parent)[1] in [MS, '']:
            return False
        logging.info('Try to restore {}'.format(input_file))
        userpath = Path(location['userpath'])
        if input_file.exists() and input_file.stat().st_size < 10:
            problem_message = 'File size < 10 bytes'
        else:
            old_path = userpath.parent
            new_path = Path(LegalPath.make(path=str(old_path)))

            if PathsBase.select().where(PathsBase.old_path == str(old_path)).exists():
                logging.info('{} Already in PathBase'.format(userpath))
            else:
                physics_path = physical_search_files(userpath, file_size)
                if physics_path:
                    PathsBase.create(old_path=str(old_path), physics_path=str(physics_path), new_path=str(new_path))
                    logging.info(
                        'Path SUCCESSFUL restored\nold_path: {}\nphysics_path: {}\nnew_path: {}'.format(old_path,
                                                                                                        physics_path,
                                                                                                        new_path))
                    item['data']['video'][0]['file']['locations'] = []
                    item['data']['video'][0]['file']['locations'].append(location)
                    return item, userpath, problem_message
    return {}, '', problem_message


def physical_search_files(userpath, file_size):
    from glob import glob
    path = Path(userpath)
    file_name = path.name
    while True:
        if not MediaLocation.joinpath(path).exists() or str(path)[-1:] == '.':
            path = path.parent
        else:
            break
    try:
        list_of_files = glob('{}/**/{}'.format(MediaLocation.joinpath(path), file_name))
    except BaseException as e:
        logging.error('Can`t find files. glob.glob error: {}'.format(repr(e)))
        list_of_files = []
    if len(list_of_files) == 0:
        logging.error('NO FILES Found !!!')
        return ''
    if len(list_of_files) == 1:
        path = Path(list_of_files[0])
    else:
        logging.warning('Found more then 1 file!!!\n{}'.format(list_of_files))
        for file in list_of_files:
            if abs(int(Path(file).stat().st_size) - file_size) < 10:
                path = file
                break
    physics_path = Path(relpath(path, MediaLocation))
    return physics_path.parent


def es_copy_file(item, file_id, dst_file):
    payload = dict(move_operation_list=list(), copy_operation_list=list(), delete_operation_list=list(),
                   operation_status="Initial",
                   percent_complete=0,
                   total_bytes_transfered=0,
                   total_file_count=0,
                   total_files_complete=0,
                   total_files_failed=0,
                   total_transfer_size=0)
    url = '{}}/transfer/copy'.format(srv_transfer)
    body = {
        "source_file_id": file_id,
        "destination_mediaspace": MS,
        "destination_path": dst_file,
        "user": "robot",
        "pass": "robot123",
        "operation_priority": "High",
        "operation_status": "Initial",
        "overwrite_flag": 'true',
        "partial_copy": "false"
    }
    payload['copy_operation_list'].append(body)

    try:
        r = s_transfer.post(url, json=payload, timeout=600)
        r.raise_for_status()
    except BaseException as e:
        logging.error('Request "COPY" failed for {}. Error: {}'.format(item, repr(e)))
    else:
        time.sleep(1)
        transfer_id = r.json()[0]['transfer']
        logging.info('Transfer_id={}'.format(transfer_id))
        state, complete = None, None
        for i in range(600):
            try:
                r = s_transfer.get('{}/{}'.format(url, transfer_id), timeout=10)
                r.raise_for_status()
            except BaseException as e:
                logging.error('Failed to get COPY status for {}. Error: {}'.format(transfer_id, repr(e)))
                return
            else:
                time.sleep(1)
                data = r.json()[0]
                if state != data['operation_status']:
                    state = data['operation_status']
                    if state == 'Complete':
                        logging.info('Successful COPY for "{}" '.format(transfer_id))
                        return True
                    elif state == 'Failed':
                        logging.warning('Failed COPY for "{}".'.format(transfer_id))
                        return
                    elif state == 'Running':
                        logging.info('COPY in process "{}" for {}'.format(state, transfer_id))
                    else:
                        logging.error('Unknown state: {} for {}'.format(state, transfer_id))
                        logging.debug(data)
                        return
            time.sleep(10)
        logging.warning('Waiting time is over 10 min for {}'.format(transfer_id))


def es_search_files(date):
    url = '{}/search/cached'.format(srv_api)
    data = {
        "combine": "MATCH_ALL",
        "filters": [
            {
                "field": {
                    "fixed_field": "MEDIA_SPACES_NAMES",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": MS
            },
            {
                "field": {
                    "fixed_field": "VIDEO_CODEC_TYPE",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "IS_NOT_EQUAL_TO",
                "search": "H.264"
            },
            {
                "field": {
                    "fixed_field": "STATUS",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": "online"
            },
            {
                "field": {
                    "fixed_field": "CREATED",
                    "group": "SEARCH_FILES",
                    "type": "QDate"
                },
                "match": "LESS_THAN",
                "search": date
            }
        ]
    }
    try:
        r = s_api.post(url, json=data, timeout=300)
        r.raise_for_status()
    except BaseException as e:
        logging.error('Search request error: {}'.format(repr(e)))
        return '', 0
    else:
        time.sleep(1)
        try:
            cache_id = r.json()['cache_id']
            results = r.json()['results']
        except BaseException as e:
            logging.error(repr(e))
            logging.error('NO FILES For DATE less then {} '.format(date))
            return '', 0
        logging.info('For {} got data {}'.format(date, r.json()))
        return cache_id, int(results)


def build_search_results(cache_id):
    def parse_duration(dur):
        dur_list = dur.split(':')
        try:
            fps = eval(dur_list[4].split(' ')[0])
        except BaseException as e_:
            logging.error(repr(e_))
            dur = 0
        else:
            dur = int(dur_list[0]) * 3600 + int(dur_list[1]) * 60 + int(dur_list[2]) + int(dur_list[3]) / fps
        return dur

    url = '{}/search/cached'.format(srv_api)
    try:
        r = s_api.get('{}/{}?start=0&max_results={}'.format(url, cache_id, LimitFiles), timeout=60)
        r.raise_for_status()
    except BaseException as e:
        logging.error('Get cached search request error: {}'.format(repr(e)))
        return 0
    else:
        time.sleep(1)
        data = r.json()
        '''
        with open(Path(log_folder).joinpath('search_{}_date_{}.json'.format(cache_id, date)), "w",
                  encoding='utf-8') as jsonFile:
            json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
        '''
        db.connection()
        for item in data['results']:
            if ErrorsBase.select().where(ErrorsBase.clip_id == item['clip_id']) or \
                    'Offline' in item['data']['video'][0]['file']['status_text']:
                pass
            else:
                orig_size = int(item['data']['video'][0]['file']['file']['filesize'])
                clip_id = item['clip_id']
                duration = parse_duration(item['data']['video'][0]['timecode_duration'])
                captured = item['data']['metadata']['captured']
                # metadata = item['data']['metadata']

                try:
                    userpath = item['data']['video'][0]['file']['locations'][0]['userpath']
                except KeyError:
                    userpath = item['data']['video'][0]['file']['gone_locations'][0]['userpath']
                    item['data']['video'][0]['file']['locations'] = item['data']['video'][0]['file']['gone_locations']

                if len(json.dumps(item, indent=4, sort_keys=True, ensure_ascii=False).encode('utf-8')) >= 65535:
                    logging.critical('BIG DATA FOR clip_id={}'.format(clip_id))
                    if not ErrorsBase.select().where(ErrorsBase.clip_id == clip_id):
                        ErrorsBase.create(orig_size=orig_size, item={}, clip_id=clip_id, duration=duration,
                                          userpath=userpath, captured=captured, problem='BIG DATA')
                else:
                    new_item, new_userpath, e1 = remove_wrong_paths(item)
                    if new_userpath:
                        ReportBase.create(orig_size=orig_size, item=new_item, clip_id=clip_id, duration=duration,
                                          userpath=str(new_userpath), captured=captured)
                    else:
                        new_item, new_userpath, e2 = restore_path(item)
                        if new_userpath:
                            ReportBase.create(orig_size=orig_size, item=new_item, clip_id=clip_id, duration=duration,
                                              userpath=str(new_userpath), captured=captured)
                        else:
                            logging.critical('clip_id={}. No source files for all locations !!!'.format(clip_id))
                            if not ErrorsBase.select().where(ErrorsBase.clip_id == clip_id):
                                ErrorsBase.create(item=item, clip_id=clip_id,
                                                  userpath=userpath,
                                                  problem='{} {}'.format(e1, e2))
        return ReportBase.select().count()


def convert_size(size):
    import math
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    try:
        i = int(math.floor(math.log(size, 1024)))
        p = math.pow(1024, i)
        s = round(size / p, 2)
    except ValueError:
        i = 0
        s = size
    return '%s %s' % (s, size_name[i])


def prepare_report() -> bool:
    orig_size, dst_size = 0, 0
    db.connection()
    for i in range(6):
        if ReportBase.select().where(ReportBase.in_work).exists() and not stop_service:
            time.sleep(600)
        else:
            break
    host = get_hostname()
    for query in ReportBase.select():
        orig_size += query.orig_size
        dst_size += query.dst_size
    saved_size = convert_size(orig_size - dst_size)
    transcode_count = ReportBase.select().where(ReportBase.transcode).count()
    clip_delete_count = ReportBase.select().where(ReportBase.clip_delete).count()
    file_copy_count = ReportBase.select().where(ReportBase.file_copy).count()
    file_remove_count = ReportBase.select().where(ReportBase.file_remove).count()
    scan_count = ReportBase.select().where(ReportBase.scan).count()
    in_work_count = ReportBase.select().where(ReportBase.in_work).count()

    msg = '{}\nFor Creation Date Less then {}:\n' \
          'Total {} files\n' \
          'In work {}\n' \
          'Not Transcoded {} files\n' \
          'Not Deleted {} clips\n' \
          'Not Copied {} files\n' \
          'Not Moved {} files\n' \
          'Not Scanned {}\n' \
          'Saved space {}\nOriginal size {}\nTranscoded size {}'.format(host, ReportBase._meta.table_name,
                                                                        ItemLength, in_work_count,
                                                                        ItemLength - transcode_count,
                                                                        ItemLength - clip_delete_count,
                                                                        ItemLength - file_copy_count,
                                                                        ItemLength - file_remove_count,
                                                                        ItemLength - scan_count,
                                                                        saved_size,
                                                                        convert_size(orig_size),
                                                                        convert_size(dst_size))
    if ItemLength == scan_count:
        sendmail(message=msg, subject='DONE')
        logging.info(msg)

        return True
    else:
        query = ReportBase.select().where(~ReportBase.transcode |
                                          ~ReportBase.clip_delete |
                                          ~ReportBase.file_copy |
                                          ~ReportBase.file_remove |
                                          ~ReportBase.scan)
        msg += '\n'
        for q in query:
            userpath = q.userpath
            if not q.transcode:
                msg += '\nNOT TRANSCODED FOR {} '.format(userpath)
            elif not q.clip_delete:
                msg += '\nNOT DELEDED CLIP FOR {}'.format(userpath)
            elif not q.file_copy:
                msg += '\nNOT COPIED FILE FOR {}'.format(userpath)
            elif not q.file_remove:
                msg += '\nNOT MOVED FILE FOR {}'.format(userpath)
            elif not q.clip_delete:
                msg += '\nNOT SCANNED FOR {}'.format(userpath)
        sendmail(message=msg, subject='WARNING')
        logging.warning(msg)

        return False


def change_table(field, value, table_name=None):
    if table_name is not None:
        ReportBase._meta.table_name = table_name
    number = ReportBase.update({field: value}).execute()
    logging.warning(
        'Table {} changed.\n{} rows for field "{}" changed to "{}"'.format(table_name, number, field, value))


def get_hostname():
    import socket
    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)
        return '{}({})'.format(host_name, host_ip)
    except BaseException as e:
        logging.error("Unable to get Hostname and IP: {}".format(repr(e)))
        return 'Undetected'


def change_file_creation_time(path, created):
    if path.is_file():
        if sys.platform == "win32":
            import win32file
            import pywintypes

            c_time = time.mktime(created.timetuple())
            handle = win32file.CreateFile(
                str(path),
                win32file.GENERIC_WRITE,
                0,
                None,
                win32file.OPEN_EXISTING,
                0,
                0
            )
            py_time = pywintypes.Time(int(c_time))
            win32file.SetFileTime(handle, py_time)

            os.utime(path, (c_time, c_time))

        else:
            import subprocess
            c = datetime.strftime(created, '%Y%m%d%H%M.%S')
            subprocess.run(['/usr/bin/touch', '-a', '-m', '-t', c, str(path)])

    else:
        logging.error('{} is not a file'.format(path))


class Transcode:
    def __init__(self):
        global stop_service
        self.problem = []
        self.i = 0
        afp_soft_stop = Path(ScriptPath, 'soft_stop.json')
        try:
            stop_service = json.load(afp_soft_stop.open('r'))['stop_service']
        except (FileNotFoundError, KeyError, json.decoder.JSONDecodeError):
            stop_service = 0
            json.dump({'stop_service': stop_service}, afp_soft_stop.open('w'))
        db.connection()
        for self.i in range(1, ItemLength + 1):
            self.problem.clear()
            if stop_service:
                logging.info('Soft Stop! {} transcoded'.format(ItemLength))
                break
            self.query = ReportBase.get(ReportBase.id == self.i)
            try:
                if not self.query.in_work:
                    self.query.in_work = True
                    self.query.save()

                    if self.query.transcode:
                        Q.put(self.i)
                    elif Path(self.query.userpath) != Path():
                        if self.transcode():
                            self.query.transcode = True
                            self.query.save()
                            Q.put(self.i)
                        else:
                            self.query.in_work = False
                            self.query.save()
                            if not ErrorsBase.select().where(ErrorsBase.clip_id == self.query.clip_id):
                                ErrorsBase.create(item=self.query.item, clip_id=self.query.clip_id,
                                                  userpath=self.query.userpath,
                                                  problem=', '.join(self.problem))
                    else:
                        self.query.in_work = False
                        self.query.save()
            except BaseException as e:
                logging.error('TRANSCODE THREAD PASSED WITH ERROR: {}'.format(repr(e)))
                self.problem.append('TRANSCODE THREAD PASSED WITH ERROR: {}'.format(repr(e)))
                self.query.in_work = False
                self.query.save()
                if not ErrorsBase.select().where(ErrorsBase.clip_id == self.query.clip_id):
                    ErrorsBase.create(item=self.query.item, clip_id=self.query.clip_id,
                                      userpath=self.query.userpath,
                                      problem=', '.join(self.problem))

        false_count = ReportBase.select().where(~ReportBase.transcode).count()
        if false_count:
            logging.warning('Not all transcoded !!!! From {} fail {}'.format(ItemLength, false_count))
        else:
            logging.info('All {} transcoded'.format(ItemLength))
        db.close()

    def is_multi_audio(self, file):
        chk_string = 'ffprobe -v quiet -print_format json -show_streams {}'.format(shlex.quote(str(file)))
        _args = shlex.split(chk_string)
        try:
            p = subprocess.run(_args, capture_output=True)
            streams = json.loads(p.stdout)
            a_streams = len(list(filter(lambda x: x['codec_type'] == 'audio', streams['streams'])))
        except BaseException as e:
            logging.error('Can`t check audio channels: {}'.format(repr(e)))
            return -1
        else:
            logging.info('{}/{} Number of audio streams = {}'.format(self.i, ItemLength, a_streams))
            return int(a_streams)

    def is_multi_audio_old(self, file):
        chk_string = '{}ffprobe -v quiet -select_streams a {} ' \
                     '-show_entries stream=index -of compact=p=0:nk=1'.format(ff_path, shlex.quote(str(file)))
        _args = shlex.split(chk_string)
        try:
            p = subprocess.run(_args, capture_output=True, universal_newlines=True)
            streams = p.stdout.splitlines()
            unique = [x for j, x in enumerate(streams) if j == streams.index(x) and x != '']
        except BaseException as e:
            logging.error('Can`t check audio channels: {}'.format(repr(e)))
            return -1
        else:
            logging.info('{}/{} Number of audio streams = {}'.format(self.i, ItemLength, len(unique)))
            return len(unique)

    def is_same_length(self, orig_file, done_file, es_duration) -> bool:
        files = [orig_file, done_file]
        length = [es_duration, 0]
        for j in range(2):
            file = files[j]
            if file.exists():
                chk_string = 'ffprobe -v quiet -print_format json -show_streams {}'.format(shlex.quote(str(file)))
                _args = shlex.split(chk_string)
                try:
                    p = subprocess.run(_args, capture_output=True)
                    streams = json.loads(p.stdout)
                    duration = list(filter(lambda x: x['codec_type'] == 'video', streams['streams']))[0]['duration']
                except BaseException as e:
                    logging.error('Can`t detect duration for {}\n error: {}'.format(file, repr(e)))
                else:
                    length[j] = float(duration)

        if abs(length[0] - length[1]) <= length[0] / 20:
            logging.info('{}/{} Length check SUCCESS'.format(self.i, ItemLength))
            return True
        else:
            logging.error('{}/{} Length check ERROR:\nOrig:\t{}\nResult:\t{}'.format(
                self.i, ItemLength, length[0], length[1]))
            return False

    def transcode(self) -> bool:
        item = self.query.item
        userpath = Path(self.query.userpath)
        timecode = item['data']['video'][0]['timecode_start']

        # ___________________ detect time to reset __________________
        try:
            action_date = item['data']['asset']['custom']['field_8']
        except BaseException as e:
            logging.debug(e)
            action_date = '2100-01-01'
        else:
            if not action_date:
                action_date = '2100-01-01'
        action_date_dt = datetime.strptime(action_date, '%Y-%m-%d')

        captured = item['data']['metadata']['captured']
        captured_dt = datetime.strptime(captured, '%Y-%m-%dT%H:%M:%SZ')

        if action_date_dt.date() < captured_dt.date():
            if action_date_dt.year in range(2015, datetime.now().year):
                action_dt = datetime.combine(action_date_dt.date(), captured_dt.time())
                date_for_change = action_dt
            else:
                date_for_change = captured_dt
        elif captured_dt.year in range(2015, datetime.now().year):
            date_for_change = captured_dt
        else:
            action_dt = datetime.combine(action_date_dt.date(), captured_dt.time())
            date_for_change = action_dt
        # ___________________________________________________________________________________________________

        logging.info(
            '{}/{} START transcoding\n{}'.format(self.i, ItemLength, MediaLocation.joinpath(userpath)))
        pb_query = PathsBase.select().where(PathsBase.old_path == str(userpath.parent))
        if pb_query.exists():
            path_query = pb_query.get()
            physics_path = Path(path_query.physics_path)
            new_path = Path(path_query.new_path)

            input_file = Path(physics_path, userpath.name)
            out_file = Path(TempFolder, new_path, userpath.stem + '.mov')
        else:
            input_file = userpath
            out_file = Path(TempFolder, userpath.parent, userpath.stem + '.mov')

        os.makedirs(MediaLocation.joinpath(out_file.parent), exist_ok=True)

        tc = ':'.join(timecode.split(':')[:-1])
        if AME:
            return self.ame_transcode(input_file, out_file, tc, date_for_change)
        else:
            return self.ff_transcode(input_file, out_file, tc, date_for_change)

    def ame_transcode(self, input_file, out_file, tc, date_for_change) -> bool:
        import xmltodict
        global AME
        duration_seconds = self.query.duration
        ame_out_file = Path(out_file.parent, out_file.stem + '.mp4')
        abs_input_file = MediaLocation.joinpath(input_file)
        abs_out_file = MediaLocation.joinpath(out_file)
        c_time = datetime.strftime(date_for_change, '%Y-%m-%d %H:%M:%S')

        def build_xml(source_path, dst_path):
            from lxml import etree
            source_path = MediaLocationWin.joinpath(source_path)
            dst_path = MediaLocationWin.joinpath(dst_path)
            root = etree.Element('manifest')
            root.attrib['version'] = '1.0'
            source = etree.SubElement(root, 'SourceFilePath')
            source.text = str(source_path)
            dest = etree.SubElement(root, 'DestinationPath')
            dest.text = str(dst_path)
            preset = etree.SubElement(root, 'SourcePresetPath')
            preset.text = str(EPR)
            return etree.tostring(root, encoding='utf-8', pretty_print=True, xml_declaration=False, ).decode('latin-1')

        headers = {'Content-Type': 'text/xml;charset=utf-8',
                   'Cache-Control': 'max-age=0, no-cache, no-store, private',
                   'Content-Encoding': 'utf-8',
                   'Content-Language': 'en, ase, ru',
                   'Accept': 'text/xml;charset=utf-8',
                   'Accept-Charset': 'utf-8',
                   'Accept-Language': 'ru'}
        try:
            r = requests.get('{}/server'.format(AME_SRV), headers=headers)
            r.raise_for_status()
        except BaseException as e:
            logging.error('AME Server offline')
            logging.error(repr(e))
            sendmail(repr(e), 'AME Server offline')
            self.problem.append('AME Server offline')
            AME = False
        else:
            logging.debug(r.text)

        logging.info('{}/{} AME process starts'.format(self.i, ItemLength))
        xml_str = build_xml(input_file, ame_out_file)
        if not self.is_same_length(abs_input_file,
                                   MediaLocation.joinpath(ame_out_file),
                                   duration_seconds):
            if MediaLocation.joinpath(ame_out_file).exists():
                MediaLocation.joinpath(ame_out_file).unlink()
            try:
                r = requests.post('{}/job'.format(AME_SRV), data=xml_str, headers=headers)
                r.raise_for_status()
            except BaseException as e:
                logging.error('AME: {}'.format(repr(e)))
                self.problem.append('AME: {}'.format(repr(e)))
            else:
                data = json.loads(json.dumps(xmltodict.parse(r.content)))['payload']
                # job_id = data['JobId']
                job_status = data['JobStatus']
                logging.info(data['Details'])
                logging.debug(data)
                while data['JobStatus'] in ['Queued', 'Encoding', 'Paused']:
                    time.sleep(5)
                    r = requests.get('{}/job'.format(AME_SRV))
                    data = json.loads(json.dumps(xmltodict.parse(r.content)))['payload']
                    if job_status != data['JobStatus']:
                        job_status = data['JobStatus']
                        logging.info(data['Details'])
                    logging.debug(data)
                if job_status == 'Success':
                    logging.info('{}/{} AME {}'.format(self.i, ItemLength, data['Details']))
                else:
                    logging.error('{}/{} AME {}'.format(self.i, ItemLength, data['Details']))
                    self.problem.append('AME Transcode FAILED: {}'.format(data['Details']))
                    return False

        if self.is_same_length(abs_input_file,
                               MediaLocation.joinpath(ame_out_file),
                               duration_seconds):
            logging.info('{}/{} AME Transcoding SUCCESSFUL'.format(self.i, ItemLength))

            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-y -f mov {}'.format(ff_path,
                                                   shlex.quote(str(MediaLocation.joinpath(ame_out_file))),
                                                   shlex.quote(tc),
                                                   shlex.quote(c_time),
                                                   shlex.quote(tc),
                                                   shlex.quote(str(abs_out_file)))
            args = shlex.split(encoder_string)

            pr = subprocess.run(args, capture_output=True, universal_newlines=True, timeout=duration_seconds * 2)
            if pr.stderr:
                logging.error('{}/{} AME StreamCopy PROBLEM: '.format(self.i, ItemLength, pr.stderr))
                self.problem.append('AME StreamCopy PROBLEM: {}'.format(pr.stderr))
            if self.is_same_length(MediaLocation.joinpath(ame_out_file),
                                   abs_out_file,
                                   MediaLocation.joinpath(ame_out_file).stat().st_size):
                logging.info('{}/{} AME StreamCopy SUCCESSFUL for {}'.format(self.i, ItemLength, abs_out_file))
                if MediaLocation.joinpath(ame_out_file).exists():
                    MediaLocation.joinpath(ame_out_file).unlink()
                change_file_creation_time(abs_out_file, date_for_change)
                return True
            else:
                logging.error('{}/{} StreamCopy FAILED\n{}'.format(self.i, ItemLength,
                                                                   MediaLocation.joinpath(ame_out_file)))
                self.problem.append('AME StreamCopy Length of original and transcoded files is DIFFERENT')
                if MediaLocation.joinpath(ame_out_file).exists():
                    MediaLocation.joinpath(ame_out_file).unlink()
                if abs_out_file.exists():
                    abs_out_file.unlink()
        if MediaLocation.joinpath(ame_out_file).exists():
            MediaLocation.joinpath(ame_out_file).unlink()
        return False

    def ff_transcode(self, input_file, out_file, tc, date_for_change) -> bool:
        duration_seconds = self.query.duration
        input_file = MediaLocation.joinpath(input_file)
        out_file = MediaLocation.joinpath(out_file)
        c_time = datetime.strftime(date_for_change, '%Y-%m-%d %H:%M:%S')

        logging.info('{}/{} Transcode starting'.format(self.i, ItemLength))
        if Path(out_file).exists():
            logging.warning('File {} already transcoded'.format(out_file))
            if self.is_same_length(input_file, out_file, duration_seconds):
                return True
            else:
                logging.info('{}/{} Trying to reencode'.format(self.i, ItemLength))

        audio_channels = self.is_multi_audio(input_file)
        if audio_channels == -1:
            return False
        elif audio_channels == 0:
            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-c:v libx264 -pix_fmt yuv420p -preset medium -crf 22 -profile:v high ' \
                             '-x264opts "weightp=0:tff=1" ' \
                             '-write_tmcd 1 -gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-an ' \
                             '-y -f mov {}'.format(ff_path,
                                                   shlex.quote(str(input_file)),
                                                   shlex.quote(tc), shlex.quote(c_time), shlex.quote(tc),
                                                   shlex.quote(str(out_file)))
        elif audio_channels == 4:
            # c_filter = '[0:v]setpts=PTS-STARTPTS[v];[0:a:0][0:a:1]join=inputs=2:channel_layout=stereo[a]'
            c_filter = '[0:v]setpts=PTS-STARTPTS[v]'
            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-filter_complex {} ' \
                             '-map [v] -map 0:a:0 -map 0:a:1 -map 0:a:2 -map 0:a:3 ' \
                             ' -c:v libx264 -pix_fmt yuv420p -preset medium -crf 22 -profile:v high ' \
                             '-x264opts "weightp=0:tff=1" ' \
                             '-write_tmcd 1 -gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-map [a] -c:a aac -b:a 224k -ac 2 -ar 48000 ' \
                             '-y -f mov {}'.format(ff_path,
                                                   shlex.quote(str(input_file)),
                                                   shlex.quote(c_filter),
                                                   shlex.quote(tc),
                                                   shlex.quote(c_time),
                                                   shlex.quote(tc),
                                                   shlex.quote(str(out_file)))
        else:
            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-c:v libx264 -pix_fmt yuv420p -preset medium -crf 22 -profile:v high ' \
                             '-x264opts "weightp=0:tff=1" ' \
                             '-write_tmcd 1 -gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-c:a aac -b:a 224k -ac 2 -ar 48000 ' \
                             '-y -f mov {}'.format(ff_path,
                                                   shlex.quote(str(input_file)),
                                                   shlex.quote(tc),
                                                   shlex.quote(c_time),
                                                   shlex.quote(tc),
                                                   shlex.quote(str(out_file)))

        # ------ write to file ---------
        if sys.platform == "win32":
            Path.open(ScriptPath.joinpath('transcode.bat'), mode='w', encoding='utf-8').write(
                'chcp 65001\n' + encoder_string.replace("'", '"') + '\npause')
        else:
            Path.open(ScriptPath.joinpath('transcode.sh'), mode='w', encoding='utf-8').write(
                '#!/bin/bash\n\n' + encoder_string.replace("'", '"'))
            ScriptPath.joinpath('transcode.sh').chmod(0o755)
        # ------------------------------

        args = shlex.split(encoder_string)
        pr = subprocess.run(args, capture_output=True, universal_newlines=True, timeout=duration_seconds * 2)
        if pr.stderr:
            ignore_list = ['Application provided duration: -']
            pr_error = str(pr.stderr)
            pr_error = pr_error.encode(encoding='cp1251', errors='ignore').decode(encoding='cp1251',
                                                                                  errors='ignore')
            if ignore_list[0] not in pr_error:
                logging.error('{}/{} PROBLEM Transcoding: '.format(self.i, ItemLength, pr_error))
                self.problem.append('PROBLEM Transcoding: {}'.format(pr_error))
        if self.is_same_length(input_file, out_file, duration_seconds):
            change_file_creation_time(out_file, date_for_change)
            logging.info('{}/{} Transcoding SUCCESSFUL'.format(self.i, ItemLength))
            return True
        else:
            logging.error('{}/{} Transcode FAILED\n{}'.format(self.i, ItemLength, input_file))
            self.problem.append('Length of original and transcoded files is DIFFERENT')


class Insert:
    def __init__(self):
        db.connection()
        while True:
            self.i = Q.get()
            if self.i == -1:
                break
            self.query = ReportBase.get(ReportBase.id == self.i)
            if self.query.clip_delete and self.query.file_copy and self.query.file_remove and self.query.scan:
                self.query.in_work = False
                self.query.save()
            else:
                logging.info('{}/{} Start Ingesting'.format(self.i, ItemLength))
                try:
                    self.query.file_remove = self.remove_original(check_mov=True)  # проверка если файл был .mov
                    self.query.save()

                    self.query.file_copy, self.query.dst_size = self.physical_copy_files(
                        src_location=MediaLocation.joinpath(TempFolder),
                        dst_location=MediaLocation)
                    self.query.save()

                    if self.query.file_copy:
                        self.query.clip_delete = self.es_delete_clip()
                        self.query.save()
                        self.query.file_remove = self.remove_original()
                        self.query.save()

                        self.query.scan = self.es_scan_asset()
                        self.query.save()
                except BaseException as e:
                    logging.error('INSERT THREAD PASSED WITH ERROR: {}'.format(repr(e)))

                self.query.in_work = False
                self.query.save()
        db.close()

    def es_delete_clip(self) -> bool:
        if self.query.clip_delete:
            return True
        else:
            clip_id = self.query.clip_id
            url = '{}/clips/{}?all=true'.format(srv_api, clip_id)
            try:
                r = s_api.delete(url, timeout=60)
                r.raise_for_status()
            except BaseException as e:
                logging.error('{}/{} Delete CLIP FAILED clip_id={}\n{}'.format(self.i, ItemLength, clip_id, repr(e)))
                time.sleep(10)
                try:
                    r = s_api.get('{}/clips/{}'.format(srv_api, clip_id), timeout=60)
                    r.raise_for_status()
                except BaseException as e:
                    logging.error(repr(e))
                    logging.info('{}/{} clip_id={} Deleted successful'.format(self.i, ItemLength, clip_id))
                    return True
                else:
                    return False
            else:
                logging.info('{}/{} clip_id={} Deleted successful'.format(self.i, ItemLength, clip_id))
                return True

    def remove_original(self, check_mov=False) -> bool:
        if self.query.file_remove:
            return True
        else:
            userpath = Path(self.query.userpath)
            old_path = userpath.parent
            if PathsBase.select().where(PathsBase.old_path == str(old_path)).exists():
                physics_path = Path(
                    PathsBase.select().where(PathsBase.old_path == str(old_path)).get().physics_path)
                file_name = userpath.name
                path_to_delete = MediaLocation.joinpath(physics_path, file_name)
            else:
                path_to_delete = MediaLocation.joinpath(userpath)

            if check_mov and path_to_delete.suffix.lower() != '.mov':
                return False

            try:
                path_to_delete.unlink()
            except BaseException as e:
                logging.error(
                    '{}/{} Delete ORIG FILE FAILED\n{}\n{}'.format(self.i, ItemLength, path_to_delete, repr(e)))
                return False
            else:
                logging.info('{}/{} Deleted original'.format(self.i, ItemLength))
                return True

    def physical_copy_files(self, src_location, dst_location):
        if self.query.file_copy:
            return True, self.query.dst_size
        else:
            file_copy, dst_size = False, 0
            item_file_path = Path(self.query.userpath)
            old_path = item_file_path.parent

            if PathsBase.select().where(PathsBase.old_path == str(old_path)).exists():
                new_path = Path(PathsBase.get(PathsBase.old_path == str(old_path)).new_path)
                file_name = item_file_path.stem
                item_file_path_with_ext = new_path.joinpath(file_name + '.mov')
            else:
                item_file_path_with_ext = item_file_path.parent.joinpath(item_file_path.stem + '.mov')

            source = src_location.joinpath(item_file_path_with_ext)
            dst = dst_location.joinpath(item_file_path_with_ext)

            os.makedirs(dst.parent, exist_ok=True)
            if dst.exists():
                dst.unlink()
            try:
                shutil.copyfile(source, dst)
            except BaseException as e:
                logging.error('File Copy FAILED\n{}\nindex={}\n{}'.format(
                    MediaLocation.joinpath(source), self.i, repr(e)))
            else:
                file_copy = True
                dst_size = int(dst.stat().st_size)
                logging.info('{}/{} Copied new file SUCCESS'.format(self.i, ItemLength))

        if file_copy:
            try:
                source.unlink()
            except BaseException as e:
                logging.error(
                    'File Remove FAILED\n{}\nindex={}\n{}'.format(MediaLocation.joinpath(source), self.i, repr(e)))
            else:
                logging.info('{}/{} Removed temp file SUCCESS'.format(self.i, ItemLength))

        return file_copy, dst_size

    def es_scan_asset(self) -> bool:
        if self.query.scan:
            return True
        else:
            if self.query.dst_size > 1000000000:
                slp = 10
            elif 500000000 < self.query.dst_size < 1000000000:
                slp = 5
            elif 100000000 < self.query.dst_size < 500000000:
                slp = 3
            else:
                slp = 2

            metadata = self.es_build_meta()
            url = '{}/scan/asset'.format(srv_scan)
            try:
                r = s_scan.post(url, json=metadata, timeout=60)
                r.raise_for_status()
            except BaseException as e:
                logging.error(
                    '{}/{} Scan Request FAILED\nmetadata: {}\n{}'.format(self.i, ItemLength, metadata['files'][0],
                                                                         repr(e)))
            else:
                time.sleep(1)
                scan_id = r.json()[1:-1]
                for j in range(100):
                    time.sleep(slp)
                    try:
                        r = s_scan.get('{}/{}'.format(url, scan_id), timeout=60)
                    except BaseException as e:
                        logging.error('{}/{} Scan get status FAILED. Error: {}'.format(self.i, ItemLength, repr(e)))
                    else:
                        time.sleep(1)
                        data = r.json()
                        state = data['state']
                        if state == 'complete':
                            logging.info('{}/{} Done Ingesting to location:\n{}'.format(self.i, ItemLength,
                                                                                        MediaLocation.joinpath(
                                                                                            metadata['files'][0])))
                            return True
                        elif state == 'failed':
                            logging.error(
                                '{}/{} Scan status FAILED\nmetadata: {}'.format(self.i, ItemLength,
                                                                                metadata['files'][0]))
                            logging.error(metadata)
                            return False
                        elif state in ['in progress', 'queued']:
                            logging.info('Waiting for SCAN complete')
                        else:
                            logging.error(
                                '{}/{} Scan UNKNOWN STATE\nState: {}'.format(self.i, ItemLength, state))
                            return False

    def es_build_meta(self) -> dict:
        item = self.query.item
        old_path = Path(self.query.userpath).parent
        if PathsBase.select().where(PathsBase.old_path == str(old_path)).exists():
            new_path = Path(PathsBase.select().where(PathsBase.old_path == str(old_path)).get().new_path)
            file_name = new_path.stem
            new_file = str(new_path.joinpath(file_name + '.mov').as_posix())
        else:
            new_path = Path(self.query.userpath)
            new_file = str(new_path.parent.joinpath(new_path.stem + '.mov'))

        # _________________Если нет даты создания и сессии восстанавление____________
        captured = item['data']['metadata']['captured'].split('T')[0]
        try:
            date = item['data']['asset']['custom']['field_8']
        except KeyError:
            item['data']['asset']['custom']['field_8'] = captured
        else:
            try:
                datetime.strptime(date, '%Y-%m-%d')
            except ValueError:
                item['data']['asset']['custom']['field_8'] = captured
        # ___________________________________________________________________________
        try:
            item['data']['asset']['custom']['field_4']
        except KeyError:
            item['data']['asset']['custom']['field_4'] = item['data']['metadata']['clip_name']

        metadata = {
            "createproxy": "true",
            "files": [new_file],
            "fullscan": "false",
            "mediaspace": MS,
            "user": "robot",
            "metadata":
                {
                    "custom": item['data']['asset']['custom'],
                    "comments": item['data']['asset']['comment'],
                    'clip_name': item['data']['metadata']['clip_name'],
                    "scene": "timed"
                }
        }

        return metadata


def main(table_name: str):
    global ItemLength
    db.connection()
    host = get_hostname()
    if HELPER_MODE:
        ReportBase._meta.table_name = table_name
        query = ReportBase.select().where(~ReportBase.transcode |
                                          ~ReportBase.clip_delete |
                                          ~ReportBase.file_copy |
                                          ~ReportBase.file_remove |
                                          ~ReportBase.scan)
        if query.exists():
            ItemLength = ReportBase.select().count()
            message = 'HELPER MODE, AME={} on {}\nFor {} found {} files\n{} UNDONE'.format(AME, host, table_name,
                                                                                           ItemLength, len(query))
        else:
            return
    else:
        cache_id, results = es_search_files(date=table_name)
        time.sleep(60)
        create_sql_tables(table_name)
        ItemLength = build_search_results(cache_id)
        message = 'MAIN MODE on {}\nFor Date {} found {} files\nCache_id {}'.format(host, table_name, ItemLength,
                                                                                    cache_id)

    sendmail(message=message, subject='START')
    logging.info(message)

    # return
    db.close()

    transcode_sub = Thread(target=Transcode)
    insert_sub = Thread(target=Insert)

    transcode_sub.start()
    insert_sub.start()

    transcode_sub.join()
    Q.put(-1)
    insert_sub.join()

    db.close()
    time.sleep(10)
    db.connection()

    pr = prepare_report()

    #  ______________ боновление таблицы ошибок, если всё-таки файлы обработаны ____________________
    query = ReportBase.select().where(ReportBase.transcode &
                                      ReportBase.clip_delete &
                                      ReportBase.file_copy &
                                      ReportBase.file_remove &
                                      ReportBase.scan)
    err_list = [x.clip_id for x in query]
    eb = ErrorsBase.delete().where(ErrorsBase.clip_id.in_(err_list)).execute()
    logging.info('Repaired {} rows from ErrorsBase'.format(eb))
    db.close()
    return pr


sys.excepthook = excepthook
all_done = False
db.connection()

# Add the arguments to the parser
ap.add_argument("--helper", required=False,
                default='True',
                help="Set helper mode. Default True. Otherwise master mode, is should be only one in system")
ap.add_argument("--ame", required=False,
                default='True',
                help="Use Adobe Media Encoder to transcode problem files. Default True. If not installed set False")
try:
    args = vars(ap.parse_args())
except BaseException as er:
    logging.error(er)
    ap.print_help()
    sys.exit(1)
else:
    HELPER_MODE = args['helper'].lower() in ['true', '1', 'yes']
    AME = args['ame'].lower() in ['true', '1', 'yes']


if AME:
    try:
        requests.get('{}/server'.format(AME_SRV))
    except requests.exceptions.ConnectionError:
        logging.warning('AME Server offline, set AME=False')
        AME = False

# change_table('in_work', False, '2021-09-30')

last_table_name = sorted(db.get_tables(), reverse=True)[0]
current_table_name = datetime.strftime(datetime.now(), '%Y-%m-%d')

if last_table_name == current_table_name:
    HELPER_MODE = True

if HELPER_MODE:
    main(last_table_name)
else:
    all_done = main(current_table_name)
    if not all_done and not stop_service:
        HELPER_MODE = True
        try:
            requests.get('{}/server'.format(AME_SRV))
        except requests.exceptions.ConnectionError:
            logging.warning('AME Server offline, set AME=False')
            AME = False
        else:
            AME = True
        main(current_table_name)
