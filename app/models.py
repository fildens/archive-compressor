import json

from peewee import *

proxy = Proxy()


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
        database = proxy
        table_name = ''
        # legacy_table_names = False


class ErrorsBase(Model):
    item = JSONField(null=False)
    userpath = TextField(default='')
    clip_id = IntegerField(default='')
    problem = TextField(default='No')

    class Meta:
        database = proxy
        table_name = '!_errors_!'


class PathsBase(Model):
    old_path = TextField(default='')
    physics_path = TextField(default='')
    new_path = TextField(default='')

    class Meta:
        database = proxy
        table_name = '!_paths_!'


def create_sql_tables(name):
    with proxy:
        ReportBase._meta.table_name = name
        ReportBase.create_table()
        ErrorsBase.create_table()
        PathsBase.create_table()
