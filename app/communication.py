import logging
import time

from app import models as db


class Msg:
    def __init__(self, c):
        self.c = c

    def sendmail(self, message, subject):
        from email.mime.text import MIMEText
        import smtplib
        me = 'trouble@tv.ru'
        you = 'dfilippov@78.ru'
        # smtp_server = 'mail.78.ru'
        smtp_server = '10.78.2.247'
        msg_ = MIMEText(message)
        msg_['Subject'] = '{} {}'.format(self.c.ScriptName.upper(), subject)
        msg_['From'] = me
        msg_['To'] = you
        try:
            s = smtplib.SMTP(smtp_server, timeout=5)
            s.sendmail(me, [you], msg_.as_string())
            s.quit()
        except BaseException as e:
            logging.error('Can`t sent email: {}'.format(repr(e)))
            logging.info('\nsubject: {}\nmessage:"{}"'.format(subject, message))

    def prepare_report(self) -> bool:
        db.proxy.initialize(self.c.arc_compress)
        orig_size, dst_size = 0, 0
        db.proxy.connection()
        for i in range(6):
            if db.ReportBase.select().where(db.ReportBase.in_work).exists() and not self.c.stop_service:
                time.sleep(600)
            else:
                break
        host = self.get_hostname()
        for query in db.ReportBase.select():
            orig_size += query.orig_size
            dst_size += query.dst_size
        saved_size = self.convert_size(orig_size - dst_size)
        transcode_count = db.ReportBase.select().where(db.ReportBase.transcode).count()
        clip_delete_count = db.ReportBase.select().where(db.ReportBase.clip_delete).count()
        file_copy_count = db.ReportBase.select().where(db.ReportBase.file_copy).count()
        file_remove_count = db.ReportBase.select().where(db.ReportBase.file_remove).count()
        scan_count = db.ReportBase.select().where(db.ReportBase.scan).count()
        in_work_count = db.ReportBase.select().where(db.ReportBase.in_work).count()

        msg = '{}\nFor Creation Date Less then {}:\n' \
              'Total {} files\n' \
              'In work {}\n' \
              'Not Transcoded {} files\n' \
              'Not Deleted {} clips\n' \
              'Not Copied {} files\n' \
              'Not Moved {} files\n' \
              'Not Scanned {}\n' \
              'Saved space {}\nOriginal size {}\nTranscoded size {}'.format(host, db.ReportBase._meta.table_name,
                                                                            self.c.ItemLength, in_work_count,
                                                                            self.c.ItemLength - transcode_count,
                                                                            self.c.ItemLength - clip_delete_count,
                                                                            self.c.ItemLength - file_copy_count,
                                                                            self.c.ItemLength - file_remove_count,
                                                                            self.c.ItemLength - scan_count,
                                                                            saved_size,
                                                                            self.convert_size(orig_size),
                                                                            self.convert_size(dst_size))
        if self.c.ItemLength == scan_count:
            self.sendmail(message=msg, subject='DONE')
            logging.info(msg)

            return True
        else:
            query = db.ReportBase.select().where(~db.ReportBase.transcode |
                                                 ~db.ReportBase.clip_delete |
                                                 ~db.ReportBase.file_copy |
                                                 ~db.ReportBase.file_remove |
                                                 ~db.ReportBase.scan)
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
            self.sendmail(message=msg, subject='WARNING')
            logging.warning(msg)

            return False

    @staticmethod
    def get_hostname():
        import socket
        try:
            host_name = socket.gethostname()
            host_ip = socket.gethostbyname(host_name)
            return '{}({})'.format(host_name, host_ip)
        except BaseException as e:
            logging.error("Unable to get Hostname and IP: {}".format(repr(e)))
            return 'Undetected'

    @staticmethod
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
