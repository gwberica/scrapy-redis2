from pymongo import MongoClient
from .global_value import gol
import copy


MONGODB_DEFAULT_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': None,
    'password': None,
    'document_class': dict,
    'tz_aware': None,
    'connect': None,
    'maxPoolSize': 30,
    'minPoolSize': 1,
    'maxIdleTimeMS': None,
    'socketTimeoutMS': 3000,
    'connectTimeoutMS': 5000,
    'serverSelectionTimeoutMS': 10000,
    'waitQueueTimeoutMS': None,
    'waitQueueMultiple': None,
    'appname': 'crawler_scheduler',
    'retryWrites': False
}


def init_mongodb(cfg):
    mongo_cfg = copy.copy(MONGODB_DEFAULT_CONFIG)
    for key in mongo_cfg:
        if key in cfg:
            mongo_cfg[key] = cfg[key]
    # host = mongo_cfg['host']
    # port = mongo_cfg['port']
    # document_class = mongo_cfg['document_class']
    # tz_aware = mongo_cfg['tz_aware']
    # connect = mongo_cfg['connect']
    # mongo_cfg.pop('host')
    # mongo_cfg.pop('port')
    # mongo_cfg.pop('document_class')
    # mongo_cfg.pop('tz_aware')
    # mongo_cfg.pop('connect')
    client = MongoClient(**mongo_cfg)
    gol.set_value('mongodb', client)


def get_mongodb(datebase):
    client = gol.get_value('mongodb')
    if client is None:
        raise ValueError("_global_dict not have mongodb's handle!")
    db = client[datebase]
    return db


class SetStatement(object):
    _statement = {}
    _need_null = False
    _del_keys = []

    @property
    def statement(self):
        return self._statement

    def __init__(self, item, need_null=False):
        self._statement = {}
        self._need_null = need_null
        self._processed(item)
        self._del_keys = ['', ' ']

    def _check_dict(self, data):
        for key in self._del_keys:
            if key in data:
                data.pop(key)

    def _on_list(self, path, data):
        self._statement[path] = data

    def _on_dict(self, path, data):
        if len(data) == 0:
            self._statement[path] = {}
            return
        # self._check_dict(data)
        for key, val in data.items():
            key = key.strip(' \r\n\t')
            if key == '':
                continue
            if path:
                cur_path = path + '.' + key
            else:
                cur_path = key
            if val is None:
                if self._need_null:
                    self._statement[cur_path] = None
                else:
                    continue
            elif isinstance(val, dict):
                self._on_dict(cur_path, val)
            elif isinstance(val, list):
                self._on_list(cur_path, val)
            else:
                self._statement[cur_path] = val

    def _processed(self, item):
        self._check_dict(item)
        for key, val in item.items():
            if key == '_id':
                continue
            elif val is None:
                if self._need_null:
                    self._statement[key] = None
                else:
                    continue
            elif isinstance(val, dict):
                self._on_dict(key, val)
            elif isinstance(val, list):
                self._on_list(key, val)
            else:
                self._statement[key] = val


