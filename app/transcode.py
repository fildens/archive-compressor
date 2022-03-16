import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import shlex
from app import models as db
import requests
from app.communication import Msg


class Transcode:
    def __init__(self, c):
        self.c = c
        self.m = Msg(c)
        db.proxy.initialize(c.arc_compress)
        self.problem = []
        self.i = 0
        afp_soft_stop = Path(self.c.ScriptPath, 'soft_stop.json')
        try:
            self.c.stop_service = json.load(afp_soft_stop.open('r'))['stop_service']
        except (FileNotFoundError, KeyError, json.decoder.JSONDecodeError):
            self.c.stop_service = 0
            json.dump({'stop_service': self.c.stop_service}, afp_soft_stop.open('w'))
        db.proxy.connection()
        for self.i in range(1, self.c.ItemLength + 1):
            self.problem.clear()
            if self.c.stop_service:
                logging.info('Soft Stop! {} transcoded'.format(self.c.ItemLength))
                break
            self.query = db.ReportBase.get(db.ReportBase.id == self.i)
            try:
                if not self.query.in_work:
                    self.query.in_work = True
                    self.query.save()

                    if self.query.transcode:
                        self.c.Q.put(self.i)
                    elif Path(self.query.userpath) != Path():
                        if self.transcode():
                            self.query.transcode = True
                            self.query.save()
                            self.c.Q.put(self.i)
                        else:
                            self.query.in_work = False
                            self.query.save()
                            if not db.ErrorsBase.select().where(db.ErrorsBase.clip_id == self.query.clip_id):
                                db.ErrorsBase.create(item=self.query.item, clip_id=self.query.clip_id,
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
                if not db.ErrorsBase.select().where(db.ErrorsBase.clip_id == self.query.clip_id):
                    db.ErrorsBase.create(item=self.query.item, clip_id=self.query.clip_id,
                                         userpath=self.query.userpath,
                                         problem=', '.join(self.problem))

        false_count = db.ReportBase.select().where(~db.ReportBase.transcode).count()
        if false_count:
            logging.warning('Not all transcoded !!!! From {} fail {}'.format(self.c.ItemLength, false_count))
        else:
            logging.info('All {} transcoded'.format(self.c.ItemLength))
        db.proxy.close()

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
            logging.info('{}/{} Number of audio streams = {}'.format(self.i, self.c.ItemLength, a_streams))
            return int(a_streams)

    def is_multi_audio_old(self, file):
        chk_string = '{}ffprobe -v quiet -select_streams a {} ' \
                     '-show_entries stream=index -of compact=p=0:nk=1'.format(self.c.ff_path, shlex.quote(str(file)))
        _args = shlex.split(chk_string)
        try:
            p = subprocess.run(_args, capture_output=True, universal_newlines=True)
            streams = p.stdout.splitlines()
            unique = [x for j, x in enumerate(streams) if j == streams.index(x) and x != '']
        except BaseException as e:
            logging.error('Can`t check audio channels: {}'.format(repr(e)))
            return -1
        else:
            logging.info('{}/{} Number of audio streams = {}'.format(self.i, self.c.ItemLength, len(unique)))
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
            logging.info('{}/{} Length check SUCCESS'.format(self.i, self.c.ItemLength))
            return True
        else:
            logging.error('{}/{} Length check ERROR:\nOrig:\t{}\nResult:\t{}'.format(
                self.i, self.c.ItemLength, length[0], length[1]))
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
            '{}/{} START transcoding\n{}'.format(self.i, self.c.ItemLength, self.c.MediaLocation.joinpath(userpath)))
        pb_query = db.PathsBase.select().where(db.PathsBase.old_path == str(userpath.parent))
        if pb_query.exists():
            path_query = pb_query.get()
            physics_path = Path(path_query.physics_path)
            new_path = Path(path_query.new_path)

            input_file = Path(physics_path, userpath.name)
            out_file = Path(self.c.TempFolder, new_path, userpath.stem + '.mov')
        else:
            input_file = userpath
            out_file = Path(self.c.TempFolder, userpath.parent, userpath.stem + '.mov')

        os.makedirs(self.c.MediaLocation.joinpath(out_file.parent), exist_ok=True)

        tc = ':'.join(timecode.split(':')[:-1])
        if self.c.AME:
            return self.ame_transcode(input_file, out_file, tc, date_for_change)
        else:
            return self.ff_transcode(input_file, out_file, tc, date_for_change)

    def ame_transcode(self, input_file, out_file, tc, date_for_change) -> bool:
        import xmltodict
        duration_seconds = self.query.duration
        ame_out_file = Path(out_file.parent, out_file.stem + '.mp4')
        abs_input_file = self.c.MediaLocation.joinpath(input_file)
        abs_out_file = self.c.MediaLocation.joinpath(out_file)
        c_time = datetime.strftime(date_for_change, '%Y-%m-%d %H:%M:%S')

        def build_xml(source_path, dst_path):
            from lxml import etree
            source_path = self.c.MediaLocationWin.joinpath(source_path)
            dst_path = self.c.MediaLocationWin.joinpath(dst_path)
            root = etree.Element('manifest')
            root.attrib['version'] = '1.0'
            source = etree.SubElement(root, 'SourceFilePath')
            source.text = str(source_path)
            dest = etree.SubElement(root, 'DestinationPath')
            dest.text = str(dst_path)
            preset = etree.SubElement(root, 'SourcePresetPath')
            preset.text = str(self.c.EPR)
            return etree.tostring(root, encoding='utf-8', pretty_print=True, xml_declaration=False, ).decode('latin-1')

        headers = {'Content-Type': 'text/xml;charset=utf-8',
                   'Cache-Control': 'max-age=0, no-cache, no-store, private',
                   'Content-Encoding': 'utf-8',
                   'Content-Language': 'en, ase, ru',
                   'Accept': 'text/xml;charset=utf-8',
                   'Accept-Charset': 'utf-8',
                   'Accept-Language': 'ru'}
        try:
            r = requests.get('{}/server'.format(self.c.AME_SRV), headers=headers)
            r.raise_for_status()
        except BaseException as e:
            logging.error('AME Server offline')
            logging.error(repr(e))
            self.m.sendmail(repr(e), 'AME Server offline')
            self.problem.append('AME Server offline')
            self.c.AME = False
        else:
            logging.debug(r.text)

        logging.info('{}/{} AME process starts'.format(self.i, self.c.ItemLength))
        xml_str = build_xml(input_file, ame_out_file)
        if not self.is_same_length(abs_input_file,
                                   self.c.MediaLocation.joinpath(ame_out_file),
                                   duration_seconds):
            if self.c.MediaLocation.joinpath(ame_out_file).exists():
                self.c.MediaLocation.joinpath(ame_out_file).unlink()
            try:
                r = requests.post('{}/job'.format(self.c.AME_SRV), data=xml_str, headers=headers)
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
                    r = requests.get('{}/job'.format(self.c.AME_SRV))
                    data = json.loads(json.dumps(xmltodict.parse(r.content)))['payload']
                    if job_status != data['JobStatus']:
                        job_status = data['JobStatus']
                        logging.info(data['Details'])
                    logging.debug(data)
                if job_status == 'Success':
                    logging.info('{}/{} AME {}'.format(self.i, self.c.ItemLength, data['Details']))
                else:
                    logging.error('{}/{} AME {}'.format(self.i, self.c.ItemLength, data['Details']))
                    self.problem.append('AME Transcode FAILED: {}'.format(data['Details']))
                    return False

        if self.is_same_length(abs_input_file,
                               self.c.MediaLocation.joinpath(ame_out_file),
                               duration_seconds):
            logging.info('{}/{} AME Transcoding SUCCESSFUL'.format(self.i, self.c.ItemLength))

            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-y -f mov {}'.format(self.c.ff_path,
                                                   shlex.quote(str(self.c.MediaLocation.joinpath(ame_out_file))),
                                                   shlex.quote(tc),
                                                   shlex.quote(c_time),
                                                   shlex.quote(tc),
                                                   shlex.quote(str(abs_out_file)))
            args = shlex.split(encoder_string)

            pr = subprocess.run(args, capture_output=True, universal_newlines=True, timeout=duration_seconds * 2)
            if pr.stderr:
                logging.error('{}/{} AME StreamCopy PROBLEM: '.format(self.i, self.c.ItemLength, pr.stderr))
                self.problem.append('AME StreamCopy PROBLEM: {}'.format(pr.stderr))
            if self.is_same_length(self.c.MediaLocation.joinpath(ame_out_file),
                                   abs_out_file,
                                   self.c.MediaLocation.joinpath(ame_out_file).stat().st_size):
                logging.info('{}/{} AME StreamCopy SUCCESSFUL for {}'.format(self.i, self.c.ItemLength, abs_out_file))
                if self.c.MediaLocation.joinpath(ame_out_file).exists():
                    self.c.MediaLocation.joinpath(ame_out_file).unlink()
                change_file_creation_time(abs_out_file, date_for_change)
                return True
            else:
                logging.error('{}/{} StreamCopy FAILED\n{}'.format(self.i, self.c.ItemLength,
                                                                   self.c.MediaLocation.joinpath(ame_out_file)))
                self.problem.append('AME StreamCopy Length of original and transcoded files is DIFFERENT')
                if self.c.MediaLocation.joinpath(ame_out_file).exists():
                    self.c.MediaLocation.joinpath(ame_out_file).unlink()
                if abs_out_file.exists():
                    abs_out_file.unlink()
        if self.c.MediaLocation.joinpath(ame_out_file).exists():
            self.c.MediaLocation.joinpath(ame_out_file).unlink()
        return False

    def ff_transcode(self, input_file, out_file, tc, date_for_change) -> bool:
        duration_seconds = self.query.duration
        input_file = self.c.MediaLocation.joinpath(input_file)
        out_file = self.c.MediaLocation.joinpath(out_file)
        c_time = datetime.strftime(date_for_change, '%Y-%m-%d %H:%M:%S')

        logging.info('{}/{} Transcode starting'.format(self.i, self.c.ItemLength))
        if Path(out_file).exists():
            logging.warning('File {} already transcoded'.format(out_file))
            if self.is_same_length(input_file, out_file, duration_seconds):
                return True
            else:
                logging.info('{}/{} Trying to reencode'.format(self.i, self.c.ItemLength))

        audio_channels = self.is_multi_audio(input_file)
        if audio_channels == -1:
            return False
        elif audio_channels == 0:
            encoder_string = '{}ffmpeg -hide_banner -loglevel error -vsync 0 -i {} -c copy ' \
                             '-c:v libx264 -pix_fmt yuv420p -preset medium -crf 22 -profile:v high ' \
                             '-x264opts "weightp=0:tff=1" ' \
                             '-write_tmcd 1 -gop_timecode {} -metadata creation_time={} -metadata timecode={} ' \
                             '-an ' \
                             '-y -f mov {}'.format(self.c.ff_path,
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
                             '-y -f mov {}'.format(self.c.ff_path,
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
                             '-y -f mov {}'.format(self.c.ff_path,
                                                   shlex.quote(str(input_file)),
                                                   shlex.quote(tc),
                                                   shlex.quote(c_time),
                                                   shlex.quote(tc),
                                                   shlex.quote(str(out_file)))

        # ------ write to file ---------
        if sys.platform == "win32":
            Path.open(self.c.ScriptPath.joinpath('transcode.bat'), mode='w', encoding='utf-8').write(
                'chcp 65001\n' + encoder_string.replace("'", '"') + '\npause')
        else:
            Path.open(self.c.ScriptPath.joinpath('transcode.sh'), mode='w', encoding='utf-8').write(
                '#!/bin/bash\n\n' + encoder_string.replace("'", '"'))
            self.c.ScriptPath.joinpath('transcode.sh').chmod(0o755)
        # ------------------------------

        args = shlex.split(encoder_string)
        pr = subprocess.run(args, capture_output=True, universal_newlines=True, timeout=duration_seconds * 2)
        if pr.stderr:
            ignore_list = ['Application provided duration: -']
            pr_error = str(pr.stderr)
            pr_error = pr_error.encode(encoding='cp1251', errors='ignore').decode(encoding='cp1251',
                                                                                  errors='ignore')
            if ignore_list[0] not in pr_error:
                logging.error('{}/{} PROBLEM Transcoding: '.format(self.i, self.c.ItemLength, pr_error))
                self.problem.append('PROBLEM Transcoding: {}'.format(pr_error))
        if self.is_same_length(input_file, out_file, duration_seconds):
            change_file_creation_time(out_file, date_for_change)
            logging.info('{}/{} Transcoding SUCCESSFUL'.format(self.i, self.c.ItemLength))
            return True
        else:
            logging.error('{}/{} Transcode FAILED\n{}'.format(self.i, self.c.ItemLength, input_file))
            self.problem.append('Length of original and transcoded files is DIFFERENT')


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