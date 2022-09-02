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
    message = '{}\n--------------{}--------------\n{}\n\n'.format(dtn, c.ScriptName, errmsg)
    c.Q.put([{'END'}, 666])
    m.sendmail(message, 'ERROR')
    m.prepare_report()
    os.kill(os.getpid(), signal.SIGTERM)


def remove_wrong_paths(item):
    number_locations = len(item['data']['video'][0]['file']['locations'])
    clip_id = item['clip_id']
    if number_locations > 1:
        logging.warning('Found more then ONE LOCATIONS for filename {}'.format(item['data']['display_name']))
    userpath_real = ''
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        userpath = location['userpath']
        input_file = c.MediaLocation.joinpath(userpath)
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
    problem_message = 'No source file for all locations'
    number_locations = len(item['data']['video'][0]['file']['locations'])
    file_size = item['data']['video'][0]['file']['file']['filesize']
    list_userpaths = list()
    for j in range(number_locations):
        location = item['data']['video'][0]['file']['locations'].pop(0)
        list_userpaths.append(location['userpath'])
        input_file = c.MediaLocation.joinpath(location['userpath'])
        if os.path.split(input_file.parent)[1] in [c.MS, '']:
            return False
        logging.info('Try to restore {}'.format(input_file))
        userpath = Path(location['userpath'])
        if input_file.exists() and input_file.stat().st_size < 10:
            problem_message = 'File size < 10 bytes'
        else:
            old_path = userpath.parent
            new_path = Path(LegalPath.make(path=str(old_path)))

            if db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).exists():
                logging.info('{} Already in PathBase'.format(userpath))
            else:
                physics_path = physical_search_files(userpath, file_size)
                if physics_path:
                    db.PathsBase.create(old_path=str(old_path), physics_path=str(physics_path), new_path=str(new_path))
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
        if not c.MediaLocation.joinpath(path).exists() or str(path)[-1:] == '.':
            path = path.parent
        else:
            break
    try:
        list_of_files = glob('{}/**/{}'.format(c.MediaLocation.joinpath(path), file_name))
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
    url = '{}}/transfer/copy'.format(c.srv_transfer)
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
    except BaseException as e:
        logging.error('Request "COPY" failed for {}. Error: {}'.format(item, repr(e)))
    else:
        time.sleep(1)
        transfer_id = r.json()[0]['transfer']
        logging.info('Transfer_id={}'.format(transfer_id))
        state, complete = None, None
        for i in range(600):
            try:
                r = c.s_transfer.get('{}/{}'.format(url, transfer_id), timeout=10)
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
    url = '{}/search/cached'.format(c.srv_api)
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
    try:
        r = c.s_api.post(url, json=data, timeout=300)
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

    url = '{}/search/cached'.format(c.srv_api)
    try:
        r = c.s_api.get('{}/{}?start=0&max_results={}'.format(url, cache_id, c.LimitFiles), timeout=60)
        r.raise_for_status()
    except BaseException as e:
        logging.error('Get cached search request error: {}'.format(repr(e)))
        return 0
    else:
        time.sleep(1)
        data = r.json()

        # with open(Path(log_folder).joinpath('search_{}_date_{}.json'.format(cache_id, date)), "w",
        #           encoding='utf-8') as jsonFile:
        #    json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)

        db.proxy.connection()
        for item in data['results']:
            if db.ErrorsBase.select().where(db.ErrorsBase.clip_id == item['clip_id']) or \
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
                            logging.critical('clip_id={}. No source files for all locations !!!'.format(clip_id))
                            if not db.ErrorsBase.select().where(db.ErrorsBase.clip_id == clip_id):
                                db.ErrorsBase.create(item=item, clip_id=clip_id,
                                                     userpath=userpath,
                                                     problem='{} {}'.format(e1, e2))
        return db.ReportBase.select().count()


def change_table(field, value, table_name=None):
    if table_name is not None:
        db.ReportBase._meta.table_name = table_name
    number = db.ReportBase.update({field: value}).execute()
    logging.warning(
        'Table {} changed.\n{} rows for field "{}" changed to "{}"'.format(table_name, number, field, value))


def main(table_name: str):
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
            message = 'HELPER MODE, AME={} on {}\nFor {} found {} files\n{} UNDONE'.format(c.AME, host, table_name,
                                                                                           c.ItemLength, len(query))
        else:
            return
    else:
        # cache_id, results = es_search_files(date=table_name)
        cache_id, results = es_search_files(date='2021-08-12')
        time.sleep(60)
        db.create_sql_tables(table_name)
        c.ItemLength = build_search_results(cache_id)
        message = 'MAIN MODE on {}\nFor Date {} found {} files\nCache_id {}'.format(host, table_name, c.ItemLength,
                                                                                    cache_id)

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
    logging.info('Repaired {} rows from db.ErrorsBase'.format(eb))
    db.proxy.close()
    return pr


sys.excepthook = excepthook
all_done = False
db.proxy.connection()

# Add the arguments to the parser
ap.add_argument("--helper", required=False,
                default='True',
                help="Set helper mode. Default True. Otherwise master mode, is should be only one in system")
ap.add_argument("--ame", required=False,
                default='False',
                help="Use Adobe Media Encoder to transcode problem files. Default True. If not installed set False")
try:
    args = vars(ap.parse_args())
except BaseException as er:
    logging.error(er)
    ap.print_help()
    sys.exit(1)
else:
    c.HELPER_MODE = args['helper'].lower() in ['true', '1', 'yes']
    c.AME = args['ame'].lower() in ['true', '1', 'yes']

if c.AME:
    try:
        requests.get('{}/server'.format(c.AME_SRV))
    except requests.exceptions.ConnectionError:
        logging.warning('AME Server offline, set AME=False')
        c.AME = False

# change_table('in_work', False, '2022-08-31')

last_table_name = sorted(db.proxy.get_tables(), reverse=True)[0]
current_table_name = datetime.strftime(datetime.now(), '%Y-%m-%d')
# last_table_name = current_table_name = '2022-08-17'

if last_table_name == current_table_name:
    c.HELPER_MODE = True

if c.HELPER_MODE:
    main(last_table_name)
else:
    all_done = main(current_table_name)
    if not all_done and not c.stop_service:
        c.HELPER_MODE = True
        try:
            requests.get('{}/server'.format(c.AME_SRV))
        except requests.exceptions.ConnectionError:
            logging.warning('AME Server offline, set AME=False')
            c.AME = False
        else:
            c.AME = True
        main(current_table_name)
# ARC_Compressor.py --helper=false --ame=true
