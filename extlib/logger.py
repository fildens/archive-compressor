# -*- coding: cp1251 -*-
# version 1.2

import logging
import os
import sys
from datetime import *
from logging.handlers import TimedRotatingFileHandler

import chardet
from dateutil.relativedelta import *
from peewee import *

db = MySQLDatabase(
    'logs_db',
    user='admin',
    password='12345Zz',
    host='192.168.13.144',
    charset='utf8')


class LogsTable(Model):
    date = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    loggername = CharField(255, unique=False, null=True)
    srclineno = IntegerField(unique=False, null=True)
    func = CharField(255, unique=False, null=True)
    level = CharField(255, unique=False, null=True)
    msg = TextField(unique=False, null=True)

    class Meta:
        database = db
        table_name = "none"


class SQLiteHandler(logging.Handler):
    def __init__(self, scriptname, months=-1):
        self.months = months
        LogsTable._meta.table_name = scriptname.lower()
        logging.Handler.__init__(self)
        try:
            db.connect()
        except OperationalError:
            logging.error('DB is not response')
            return
        else:
            if not db.table_exists(LogsTable._meta.table_name):
                LogsTable.create_table()
            db.close()

    def emit(self, record):

        msg = record.msg
        msg = str(msg)

        enc = (chardet.detect(msg.encode()))['encoding']
        if enc is not None:
            msg_orig = msg.encode(enc, errors='ignore').decode(enc, errors='ignore')
            msg_sql = msg_orig.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
        else:
            msg_sql = msg.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
        db.connection()

        LogsTable.create(
            loggername=record.name,
            srclineno=record.lineno,
            func=record.funcName,
            level=record.levelname,
            msg=msg_sql
        )
        dtb = datetime.now() + relativedelta(months=self.months)
        dtb = dtb.strftime('%Y-%m-%d %H:%M:%S')
        LogsTable.delete().where(LogsTable.date < dtb).execute()

        db.close()


class Logger:
    def __init__(self, script_name, script_path, log_path=None, level='INFO', sql=False, err_log=False):
        if log_path:
            self.log_path = log_path
        else:
            self.log_path = os.path.join(script_path, 'LOGS')
        os.makedirs(self.log_path, exist_ok=True)

        self.script_name = script_name

        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('peewee').setLevel(logging.ERROR)
        logging.getLogger('chardet.charsetprober').setLevel(logging.ERROR)

        self.logger = logging.getLogger()
        if level == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        console_formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s', '%d.%m %H:%M')
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        log_file_handler_filename = os.path.join(self.log_path, script_name + '.log')
        log_file_handler = TimedRotatingFileHandler(log_file_handler_filename,
                                                    when='W0',
                                                    backupCount=5,
                                                    encoding='cp1251')
        log_file_formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)d %(message)s',
            '%Y-%m-%d %H:%M:%S')
        log_file_handler.setFormatter(log_file_formatter)

        if err_log:
            err_log_file_handler_filename = os.path.join(self.log_path, script_name + '.err')
            self.err_log_file_handler = TimedRotatingFileHandler(err_log_file_handler_filename,
                                                                 when='midnight',
                                                                 backupCount=7,
                                                                 encoding='cp1251')

            self.err_log_file_handler.setFormatter(log_file_formatter)
            self.err_log_file_handler.setLevel(logging.ERROR)
            self.logger.addHandler(self.err_log_file_handler)

        self.logger.addHandler(log_file_handler)
        if sql:
            self.logger.addHandler(SQLiteHandler(script_name))

    def build(self):
        return self.logger

    def do_rollover(self):
        yesterday = datetime.now() - timedelta(days=1)
        fe = os.path.join(self.log_path, '{}.err.{}'.format(self.script_name, yesterday.strftime('%Y-%m-%d')))
        le = os.path.join(self.log_path, '{}.err'.format(self.script_name))
        if not os.path.exists(fe):
            self.err_log_file_handler.doRollover()  # close error_log file to get it and send
        try:
            with open(fe, 'rb') as eh:
                errors = eh.read().decode('cp1251')
                if errors:
                    message = errors
                    subject = 'ERROR'
                else:
                    message = "Everything alright"
                    subject = 'OK'
        except FileNotFoundError:
            with open(le, 'rb') as eh:
                errors = eh.read().decode('cp1251')
                if errors:
                    message = 'File {} is not exist.\nRead from current:\n{}'.format(fe, errors)
                    subject = 'ERROR'
                else:
                    message = "Everything alright"
                    subject = 'OK'
        return message, subject
