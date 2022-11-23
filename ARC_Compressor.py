# -*- coding: cp1251 -*-

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from os.path import relpath
from pathlib import Path
from threading import Thread

import requests
import urllib3

from app import models as db
from app.communication import Msg
from app.insert import Insert
from app.params import Conf
from app.transcode import Transcode
from extlib.logger import Logger
from extlib import LegalPath

c = Conf()
m = Msg(c)

c.ScriptName = Path(__file__).stem

L = Logger(c.ScriptName, c.ScriptPath, sql=False, err_log=True)
logger = L.build()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ap = argparse.ArgumentParser()

db.proxy.initialize(c.arc_compress)


def excepthook(type_, value, traceback):
    import signal
    from traceback import format_exception
    c.Q.put(-1)
    errmsg = ''.join(format_exception(type_, value, traceback))
    logging.error(errmsg)
    dtn = datetime.now().strftime('%d.%m.%Y_%H:%M')
    message = f'{dtn}\n--------------{c.ScriptName}--------------\n{errmsg}\n\n'
    c.Q.put([{'END'}, 666])
    m.sendmail(message, 'ERROR')
    m.prepare_report()
    os.kill(os.getpid(), signal.SIGTERM)


def remove_wrong_paths(item):
    number_locations = len(item['data']['video'][0]['file']['locations'])
    clip_id = item['clip_id']
    if number_locations > 1:
        logging.warning(f"Found more then ONE LOCATIONS for filename {item['data']['display_name']}")
    userpath_real = ''
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        userpath = location['userpath']
        input_file = c.MediaLocation.joinpath(userpath)
        if input_file.exists():
            if input_file.stat().st_size < 10:
                logging.critical(f'Less 10 bytes for {input_file} clip_id={clip_id}')
            else:
                es_orig_size = int(item['data']['video'][0]['file']['file'].get('filesize', 0))
                ch_orig_size = input_file.stat().st_size

                if abs(es_orig_size - ch_orig_size) > es_orig_size / 20:
                    logging.warning(
                        f'ORIG FILE HAVE WRONG SIZE:'
                        f'\nES_Size={es_orig_size}\nStat_Size={ch_orig_size}\n'
                        f'Difference={abs(es_orig_size - ch_orig_size)}')
                item['data']['video'][0]['file']['locations'].append(location)
                userpath_real = userpath
        else:
            logging.warning(f"REMOVED wrong path {location['userpath']}")
    if len(item['data']['video'][0]['file']['locations']) == 0:
        return {}, '', ''
    else:
        return item, userpath_real, ''


def restore_path(item):
    problem_message = 'No source file for all locations'
    number_locations = len(item['data']['video'][0]['file']['locations'])
    file_size = item['data']['video'][0]['file']['file'].get('filesize', 0)
    list_userpaths = list()
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        list_userpaths.append(location['userpath'])
        input_file = c.MediaLocation.joinpath(location['userpath'])
        if os.path.split(input_file.parent)[1] in [c.MS, '']:
            return False
        logging.info(f'Try to restore {input_file}')
        userpath = Path(location['userpath'])
        if input_file.exists() and input_file.stat().st_size < 10:
            problem_message = 'File size < 10 bytes'
        else:
            old_path = userpath.parent
            new_path = Path(LegalPath.make(path=str(old_path)))

            if db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).exists():
                logging.info(f'{userpath} Already in PathBase')
            else:
                physics_path = physical_search_files(userpath, file_size)
                if physics_path:
                    db.PathsBase.create(old_path=str(old_path), physics_path=str(physics_path), new_path=str(new_path))
                    logging.info(f'Path SUCCESSFUL restored\nold_path: {old_path}\n'
                                 f'physics_path: {physics_path}\nnew_path: {new_path}')
                    item['data']['video'][0]['file']['locations'] = []
                    item['data']['video'][0]['file']['locations'].append(location)
                    return item, userpath, problem_message
    return {}, '', problem_message


def physical_search_files(userpath, file_size):
    from glob import glob
    path = Path(userpath)
    file_name = path.name
    while True:
        if not c.MediaLocation.joinpath(path).exists() or str(path)[-1:] == '.':
            path = path.parent
        else:
            break
    try:
        list_of_files = glob(f'{c.MediaLocation.joinpath(path)}/**/{file_name}')
    except BaseException as e_:
        logging.error(f'Can`t find files. glob.glob error: {repr(e_)}')
        list_of_files = []
    if len(list_of_files) == 0:
        logging.error('NO FILES Found !!!')
        return ''
    if len(list_of_files) == 1:
        path = Path(list_of_files[0])
    else:
        logging.warning(f'Found more then 1 file!!!\n{list_of_files}')
        for file in list_of_files:
            if abs(int(Path(file).stat().st_size) - file_size) < 10:
                path = file
                break
    physics_path = Path(relpath(path, c.MediaLocation))
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
    url = f'{c.srv_transfer}/transfer/copy'
    body = {
        "source_file_id": file_id,
        "destination_mediaspace": c.MS,
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
        r = c.s_transfer.post(url, json=payload, timeout=600)
        r.raise_for_status()
    except BaseException as e__:
        logging.error(f'Request "COPY" failed for {item}. Error: {repr(e__)}')
    else:
        time.sleep(1)
        transfer_id = r.json()[0]['transfer']
        logging.info(f'Transfer_id={transfer_id}')
        state, complete = None, None
        for i in range(600):
            try:
                r = c.s_transfer.get(f'{url}/{transfer_id}', timeout=10)
                r.raise_for_status()
            except BaseException as e_:
                logging.error(f'Failed to get COPY status for {transfer_id}. Error: {repr(e_)}')
                return
            else:
                time.sleep(1)
                data = r.json()[0]
                if state != data['operation_status']:
                    state = data['operation_status']
                    if state == 'Complete':
                        logging.info(f'Successful COPY for "{transfer_id}"')
                        return True
                    elif state == 'Failed':
                        logging.warning(f'Failed COPY for "{transfer_id}"')
                        return
                    elif state == 'Running':
                        logging.info(f'COPY in process "{state}" for {transfer_id}')
                    else:
                        logging.error(f'Unknown state: {state} for {transfer_id}')
                        logging.debug(data)
                        return
            time.sleep(10)
        logging.warning(f'Waiting time is over 10 min for {transfer_id}')


def es_search_files(date):
    url = f'{c.srv_api}/search'
    data_base = {
        "combine": "MATCH_ALL",
        "filters": [
            {
                "field": {
                    "fixed_field": "MEDIA_SPACES_NAMES",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": c.MS
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
    data_tmp1 = {
        "combine": "MATCH_ALL",
        "filters": [
            {
                "field": {
                    "custom_field": "field_5",
                    "fixed_field": "CUSTOM_field_5",
                    "group": "SEARCH_ASSETS",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": "SkyLark"
            },
            {
                "field": {
                    "fixed_field": "CAPTURED",
                    "group": "SEARCH_FILES",
                    "type": "QDate"
                },
                "match": "LESS_THAN",
                "search": "2022-01-01"
            },
            {
                "field": {
                    "custom_field": "field_13",
                    "fixed_field": "CUSTOM_field_13",
                    "group": "SEARCH_ASSETS",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": "АРХИВИРОВАТЬ"
            },
            {
                "field": {
                    "fixed_field": "FILE_EXT",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": "mxf"
            },
            {
                "field": {
                    "fixed_field": "MEDIA_SPACES_NAMES",
                    "group": "SEARCH_FILES",
                    "type": "QString"
                },
                "match": "EQUAL_TO",
                "search": "ARCHIVE"
            }
        ]
    }
    data = data_base
    try:
        r = c.s_api.post(url, json=data, timeout=300)
        r.raise_for_status()
    except BaseException as e_:
        logging.error(f'Search request error: {repr(e_)}')
        return '', 0
    else:
        clip_ids = [_['clip_id'] for _ in r.json()]
        logging.info(f'For less then {date} found {len(r.json())} files')
        clip_ids.sort()
        return clip_ids[:c.LimitFiles], len(r.json())


def build_search_results(clip_ids):
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

    db.proxy.connection()
    for clip_id in clip_ids:
        time.sleep(0.5)
        try:
            r = c.s_api.get(f'{c.srv_api}/clips/{clip_id}', timeout=5)
            r.raise_for_status()
        except BaseException as e__:
            logging.error(f'Get cached search request error: {repr(e__)}')
        else:
            item = dict(clip_id=clip_id, data=r.json())

            if db.ErrorsBase.select().where(db.ErrorsBase.clip_id == item['clip_id']) or \
                    'Offline' in item['data']['video'][0]['file']['status_text']:
                pass
            else:
                orig_size = int(item['data']['video'][0]['file']['file'].get('filesize', 0))
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
                    logging.critical(f'BIG DATA FOR clip_id={clip_id}')
                    if not db.ErrorsBase.select().where(db.ErrorsBase.clip_id == clip_id):
                        db.ErrorsBase.create(orig_size=orig_size, item={}, clip_id=clip_id, duration=duration,
                                             userpath=userpath, captured=captured, problem='BIG DATA')
                else:
                    new_item, new_userpath, e1 = remove_wrong_paths(item)
                    if new_userpath:
                        db.ReportBase.create(orig_size=orig_size, item=new_item, clip_id=clip_id, duration=duration,
                                             userpath=str(new_userpath), captured=captured)
                    else:
                        new_item, new_userpath, e2 = restore_path(item)
                        if new_userpath:
                            db.ReportBase.create(orig_size=orig_size, item=new_item, clip_id=clip_id, duration=duration,
                                                 userpath=str(new_userpath), captured=captured)
                        else:
                            logging.critical(f'clip_id={clip_id}. No source files for all locations !!!')
                            if not db.ErrorsBase.select().where(db.ErrorsBase.clip_id == clip_id):
                                db.ErrorsBase.create(item=item, clip_id=clip_id,
                                                     userpath=userpath,
                                                     problem=f'{e1} {e2}')
    return db.ReportBase.select().count()


def change_table(field, value, table_name=None):
    if table_name is not None:
        db.ReportBase._meta.table_name = table_name
    number = db.ReportBase.update({field: value}).execute()
    logging.warning(f'Table {table_name} changed.\n{number} rows for field "{field}" changed to "{value}"')


def main(table_name: str, date: str):
    db.proxy.connection()
    host = m.get_hostname()
    if c.HELPER_MODE:
        db.ReportBase._meta.table_name = table_name
        query = db.ReportBase.select().where(~db.ReportBase.transcode |
                                             ~db.ReportBase.clip_delete |
                                             ~db.ReportBase.file_copy |
                                             ~db.ReportBase.file_remove |
                                             ~db.ReportBase.scan)
        if query.exists():
            c.ItemLength = db.ReportBase.select().count()
            message = f'HELPER MODE, AME={c.AME} on {host}\n' \
                      f'For {table_name} found {c.ItemLength} files\n{len(query)} UNDONE'
        else:
            return
    else:
        clip_ids, results = es_search_files(date=date)
        # clip_ids, results = es_search_files(date='2022-08-12')
        db.create_sql_tables(table_name)
        c.ItemLength = build_search_results(clip_ids)
        message = f'MAIN MODE on {host}\nFor less then {date} found {c.ItemLength} files'

    m.sendmail(message=message, subject='START')
    logging.info(message)

    # return
    db.proxy.close()

    transcode_sub = Thread(target=Transcode, args=(c,))
    insert_sub = Thread(target=Insert, args=(c,))

    transcode_sub.start()
    insert_sub.start()

    transcode_sub.join()
    c.Q.put(-1)
    insert_sub.join()

    db.proxy.close()
    time.sleep(10)
    db.proxy.connection()

    pr = m.prepare_report()

    #  ______________ боновление таблицы ошибок, если всё-таки файлы обработаны ____________________
    query = db.ReportBase.select().where(db.ReportBase.transcode &
                                         db.ReportBase.clip_delete &
                                         db.ReportBase.file_copy &
                                         db.ReportBase.file_remove &
                                         db.ReportBase.scan)
    err_list = [x.clip_id for x in query]
    eb = db.ErrorsBase.delete().where(db.ErrorsBase.clip_id.in_(err_list)).execute()
    logging.info(f'Repaired {eb} rows from db.ErrorsBase')
    db.proxy.close()
    return pr


sys.excepthook = excepthook
all_done = False
db.proxy.connection()

# Add the arguments to the parser
ap.add_argument("--main", default=False,
                action='store_true', required=False,
                help="Set MAIN mode. It should be only one in system! Default False - HELPER mode.")
ap.add_argument("--ame", default=False,
                action='store_true', required=False,
                help="Use Adobe Media Encoder to transcode problem files. Default False. If not installed set False")
try:
    args = vars(ap.parse_args())
except BaseException as e:
    logging.error(e)
    ap.print_help()
    sys.exit(1)
else:
    c.HELPER_MODE, c.AME = not args['main'], args['ame']
if c.AME:
    try:
        requests.get(f'{c.AME_SRV}/server')
    except requests.exceptions.ConnectionError:
        logging.warning('AME Server offline, set AME=False')
        c.AME = False

# change_table('in_work', False, '2022-08-31')
last_table_name = sorted(db.proxy.get_tables(), reverse=True)[0]

now = datetime.now()
search_date = f'{now.year}-{(now.month - 9):02d}-{now.day:02d}'
current_table_name = datetime.strftime(datetime.now(), '%Y-%m-%d')
# last_table_name = current_table_name = '2022-08-17'

if last_table_name == current_table_name:
    c.HELPER_MODE = True

if c.HELPER_MODE:
    main(last_table_name, search_date)
else:
    all_done = main(current_table_name, search_date)
    if not all_done and not c.stop_service:
        c.HELPER_MODE = True
        try:
            requests.get(f'{c.AME_SRV}/server')
        except requests.exceptions.ConnectionError:
            logging.warning('AME Server offline, set AME=False')
            c.AME = False
        else:
            c.AME = True
        main(current_table_name, search_date)
# ARC_Compressor.py --main --ame
# it set helper=False and ame=True
