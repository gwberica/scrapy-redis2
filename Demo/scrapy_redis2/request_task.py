import time
import json
from scrapy.http import Request, Response

TASK_REQUEST_STATE_CODE_ERROR = -1
TASK_REQUEST_STATE_CODE_PROCESSED = 0
TASK_REQUEST_STATE_CODE_COMPLETED = 1
TASK_REQUEST_STATE_CODE_RETRIED = 100

'''
request中 meta 中scrapy_redis格式：
'meta': {
    'crawler_scheduler': {
            # 任务ID
            'task_id': '',
            # 任务编号(不是唯一)
            'task_no': task_no,
            # 深度
            'depth': None,
            # 优先级
            'priority': 4,
            # 请求状态
            'request_state': TASK_REQUEST_STATE_CODE_PROCESSED,
            # 请求类型
            'task_type': task_type,
            # request回调函数
            'callback': None,
            # request回调函数
            'errback': None,
            # 爬虫的服务器ID
            'worker_id': None,
            # 任务创建时间
            'create_time': int(time.time()),
            # 进入队列时间
            'enqueue_time': None,
            # 自定义参数
            'params': None
    }
}
'''


class RequestTask(object):

    _crawler_scheduler = {}

    def __init__(self, crawler_scheduler=None):
        if not crawler_scheduler:
            crawler_scheduler = {}
        elif not isinstance(crawler_scheduler, dict):
            raise ValueError("'crawler_scheduler' type isn't dict:" + str(crawler_scheduler))
        self._crawler_scheduler = crawler_scheduler

    @property
    def crawler_scheduler(self):
        return self._crawler_scheduler

    @property
    def task_id(self):
        return self._crawler_scheduler.get('task_id', None)

    @property
    def task_no(self):
        return self._crawler_scheduler.get('task_no', None)

    @property
    def depth(self):
        return self._crawler_scheduler.get('depth', None)

    @depth.setter
    def depth(self, val):
        self._crawler_scheduler['depth'] = val

    @property
    def priority(self):
        return self._crawler_scheduler.get('priority', 4)

    @property
    def task_type(self):
        return self._crawler_scheduler.get('task_type', None)

    @task_type.setter
    def task_type(self, val):
        self._crawler_scheduler['task_type'] = val

    @property
    def params(self):
        return self._crawler_scheduler.get('params', None)

    @property
    def create_time(self):
        return self._crawler_scheduler.get('create_time', None)

    @create_time.setter
    def create_time(self, val):
        self._crawler_scheduler['create_time'] = val

    @property
    def callback(self):
        return self._crawler_scheduler.get('callback', None)

    @callback.setter
    def callback(self, val):
        self._crawler_scheduler['callback'] = val

    @property
    def errback(self):
        return self._crawler_scheduler.get('errback', None)

    @errback.setter
    def errback(self, val):
        self._crawler_scheduler['errback'] = val

    @property
    def worker_id(self):
        return self._crawler_scheduler.get('worker_id', '')

    @worker_id.setter
    def worker_id(self, val):
        self._crawler_scheduler['worker_id'] = val

    @property
    def enqueue_time(self):
        return self._crawler_scheduler.get('enqueue_time', 0.0)

    @enqueue_time.setter
    def enqueue_time(self, val):
        try:
            self._crawler_scheduler['enqueue_time'] = round(val, 3)
        except:
            self._crawler_scheduler['enqueue_time'] = None

    @property
    def request_state(self):
        return self._crawler_scheduler.get('request_state', 0)

    @request_state.setter
    def request_state(self, val):
        defaule_vals = [
            TASK_REQUEST_STATE_CODE_ERROR,
            TASK_REQUEST_STATE_CODE_PROCESSED,
            TASK_REQUEST_STATE_CODE_COMPLETED,
            TASK_REQUEST_STATE_CODE_RETRIED
        ]
        if val not in defaule_vals:
            val = TASK_REQUEST_STATE_CODE_PROCESSED
        self._crawler_scheduler['request_state'] = val

    def __str__(self):
        return json.dumps(self._crawler_scheduler, ensure_ascii=False)

    @staticmethod
    def get_errback_from_request(request):
        return request.meta['crawler_scheduler'].get('errback', None)

    @staticmethod
    def get_callback_from_request(request):
        return request.meta['crawler_scheduler'].get('callback', None)

    @classmethod
    def from_request(cls, data):
        if isinstance(data, Response):
            request = data.request
            crawler_scheduler = request.meta.get('crawler_scheduler', {})
        elif isinstance(data, Request):
            request = data
            crawler_scheduler = request.meta.get('crawler_scheduler', {})
        elif isinstance(data, dict):
            try:
                meta = data['meta']
                crawler_scheduler = meta['crawler_scheduler']
            except:
                raise ValueError("Param is illegal data:" + str(data))
        else:
            raise ValueError("Param is illegal data:" + str(data))
        obj = cls(crawler_scheduler)
        return obj

    @classmethod
    def from_create(cls, task_id, task_no=None, priority=4, depth=None, task_type=None, params=None):
        crawler_scheduler = {
            # 任务ID, 唯一
            'task_id': task_id,
            # 任务编号(不是唯一)
            'task_no': task_no,
            # 深度
            'depth': depth,
            # 优先级
            'priority': priority,
            # 请求状态
            'request_state': TASK_REQUEST_STATE_CODE_PROCESSED,
            # 请求类型
            'task_type': task_type,
            # request回调函数
            'callback': None,
            # request回调函数
            'errback': None,
            # 爬虫的服务器ID
            'worker_id': None,
            # 任务创建时间
            'create_time': int(time.time()),
            # 进入队列时间
            'enqueue_time': None,
            # 自定义参数
            'params': params
        }
        return cls(crawler_scheduler)




