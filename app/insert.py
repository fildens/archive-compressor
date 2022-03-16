import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

from app import models as db


class Insert:
    def __init__(self, c):
        self.c = c
        db.proxy.initialize(c.arc_compress)
        db.proxy.connection()
        while True:
            self.i = self.c.Q.get()
            if self.i == -1:
                break
            self.query = db.ReportBase.get(db.ReportBase.id == self.i)
            if self.query.clip_delete and self.query.file_copy and self.query.file_remove and self.query.scan:
                self.query.in_work = False
                self.query.save()
            else:
                logging.info('{}/{} Start Ingesting'.format(self.i, c.ItemLength))
                try:
                    self.query.file_remove = self.remove_original(check_mov=True)  # проверка если файл был .mov
                    self.query.save()

                    self.query.file_copy, self.query.dst_size = self.physical_copy_files(
                        src_location=c.MediaLocation.joinpath(c.TempFolder),
                        dst_location=c.MediaLocation)
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
        db.proxy.close()

    def es_delete_clip(self) -> bool:
        if self.query.clip_delete:
            return True
        else:
            clip_id = self.query.clip_id
            url = '{}/clips/{}?all=true'.format(self.c.srv_api, clip_id)
            try:
                r = self.c.s_api.delete(url, timeout=60)
                r.raise_for_status()
            except BaseException as e:
                logging.error(
                    '{}/{} Delete CLIP FAILED clip_id={}\n{}'.format(self.i, self.c.ItemLength, clip_id, repr(e)))
                time.sleep(10)
                try:
                    r = self.c.s_api.get('{}/clips/{}'.format(self.c.srv_api, clip_id), timeout=60)
                    r.raise_for_status()
                except BaseException as e:
                    logging.error(repr(e))
                    logging.info('{}/{} clip_id={} Deleted successful'.format(self.i, self.c.ItemLength, clip_id))
                    return True
                else:
                    return False
            else:
                logging.info('{}/{} clip_id={} Deleted successful'.format(self.i, self.c.ItemLength, clip_id))
                return True

    def remove_original(self, check_mov=False) -> bool:
        if self.query.file_remove:
            return True
        else:
            userpath = Path(self.query.userpath)
            old_path = userpath.parent
            if db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).exists():
                physics_path = Path(
                    db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).get().physics_path)
                file_name = userpath.name
                path_to_delete = self.c.MediaLocation.joinpath(physics_path, file_name)
            else:
                path_to_delete = self.c.MediaLocation.joinpath(userpath)

            if check_mov and path_to_delete.suffix.lower() != '.mov':
                return False

            try:
                path_to_delete.unlink()
            except BaseException as e:
                logging.error(
                    '{}/{} Delete ORIG FILE FAILED\n{}\n{}'.format(self.i, self.c.ItemLength, path_to_delete, repr(e)))
                return False
            else:
                logging.info('{}/{} Deleted original'.format(self.i, self.c.ItemLength))
                return True

    def physical_copy_files(self, src_location, dst_location):
        if self.query.file_copy:
            return True, self.query.dst_size
        else:
            file_copy, dst_size = False, 0
            item_file_path = Path(self.query.userpath)
            old_path = item_file_path.parent

            if db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).exists():
                new_path = Path(db.PathsBase.get(db.PathsBase.old_path == str(old_path)).new_path)
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
                    self.c.MediaLocation.joinpath(source), self.i, repr(e)))
            else:
                file_copy = True
                dst_size = int(dst.stat().st_size)
                logging.info('{}/{} Copied new file SUCCESS'.format(self.i, self.c.ItemLength))

        if file_copy:
            try:
                source.unlink()
            except BaseException as e:
                logging.error(
                    'File Remove FAILED\n{}\nindex={}\n{}'.format(self.c.MediaLocation.joinpath(source), self.i,
                                                                  repr(e)))
            else:
                logging.info('{}/{} Removed temp file SUCCESS'.format(self.i, self.c.ItemLength))

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
            url = '{}/scan/asset'.format(self.c.srv_scan)
            try:
                r = self.c.s_scan.post(url, json=metadata, timeout=60)
                r.raise_for_status()
            except BaseException as e:
                logging.error(
                    '{}/{} Scan Request FAILED\nmetadata: {}\n{}'.format(self.i, self.c.ItemLength,
                                                                         metadata['files'][0],
                                                                         repr(e)))
            else:
                time.sleep(1)
                scan_id = r.json()[1:-1]
                for j in range(100):
                    time.sleep(slp)
                    try:
                        r = self.c.s_scan.get('{}/{}'.format(url, scan_id), timeout=60)
                    except BaseException as e:
                        logging.error(
                            '{}/{} Scan get status FAILED. Error: {}'.format(self.i, self.c.ItemLength, repr(e)))
                    else:
                        time.sleep(1)
                        data = r.json()
                        state = data['state']
                        if state == 'complete':
                            logging.info('{}/{} Done Ingesting to location:\n{}'.format(self.i, self.c.ItemLength,
                                                                                        self.c.MediaLocation.joinpath(
                                                                                            metadata['files'][0])))
                            return True
                        elif state == 'failed':
                            logging.error(
                                '{}/{} Scan status FAILED\nmetadata: {}'.format(self.i, self.c.ItemLength,
                                                                                metadata['files'][0]))
                            logging.error(metadata)
                            return False
                        elif state in ['in progress', 'queued']:
                            logging.info('Waiting for SCAN complete')
                        else:
                            logging.error(
                                '{}/{} Scan UNKNOWN STATE\nState: {}'.format(self.i, self.c.ItemLength, state))
                            return False

    def es_build_meta(self) -> dict:
        item = self.query.item
        old_path = Path(self.query.userpath).parent
        if db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).exists():
            new_path = Path(db.PathsBase.select().where(db.PathsBase.old_path == str(old_path)).get().new_path)
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
            "mediaspace": self.c.MS,
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
