import importlib
import six

from . import connection, defaults, request_task
from .schedulertask import processed_task_queue
from .dupefilter import RFPDupeFilter
from .queue import SpiderPriorityQueue


# TODO: add SCRAPY_JOB support.
class Scheduler(object):
    """Redis-based scheduler

    Settings
    --------
    SCHEDULER_SERIALIZER : str
        Scheduler serializer.
    SCHEDULER_IDLE_BEFORE_CLOSE : int (default: 0)
        How many seconds to wait before closing if no message is received.

    """

    def __init__(self, server, idle_before_close=defaults.SCHEDULER_IDLE_BEFORE_CLOSE, serializer=None):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        idle_before_close : int
            Timeout before giving up.
        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.serializer = serializer
        self.idle_before_close = idle_before_close
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE', defaults.SCHEDULER_IDLE_BEFORE_CLOSE),
        }

        # If these values are missing, it means we want to use the defaults.
        optional = {
            # TODO: Use custom prefixes for this settings to note that are
            'serializer': 'SCHEDULER_SERIALIZER',
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        # Support serializer as a path to a module.
        if isinstance(kwargs.get('serializer'), six.string_types):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        # FIXME: for now, stats are only supported from this constructor
        instance.stats = crawler.stats
        return instance

    def open(self, spider):

        self.spider = spider
        self.queue = SpiderPriorityQueue(server=self.server, spider=spider)

        # processed_queue 队列用于接收未处理request，已处理request
        self.processed_queue = processed_task_queue(self.server, spider)
        self.df = RFPDupeFilter(debug=spider.settings.getbool('DUPEFILTER_DEBUG'))

        self.work_id = spider.settings.get('SCRAPY_WORKER_ID')

        # notice if there are requests already in the queue to resume the crawl
        if len(self.queue):
            spider.log("Resuming crawl (%d requests scheduled)" % len(self.queue))

    def close(self, reason):
        pass

    def flush(self):
        self.df.clear()
        self.queue.clear()

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/redis', spider=self.spider)
        if 'retry_times' in request.meta:
            # request.meta['crawler_scheduler']['request_state'] = task_defined.TASK_REQUEST_STATE_CODE_RETRIED
            req_task = request_task.RequestTask.from_request(request)
            req_task.request_state = request_task.TASK_REQUEST_STATE_CODE_RETRIED
        self.processed_queue.push(request)
        self.spider.logger.info("Scheduler.enqueue_request -->processed_queue: " + request.url + "|" + str(request.meta))
        return True

    def next_request(self):
        block_pop_timeout = self.idle_before_close
        request = self.queue.pop(block_pop_timeout)
        if request and self.stats:
            request = self.spider.check_request_callback(request)
            if request is not None:
                # request.meta['crawler_scheduler']['worker_id'] = self.work_id
                req_task = request_task.RequestTask.from_request(request)
                req_task.worker_id = self.work_id
                self.stats.inc_value('scheduler/dequeued/redis', spider=self.spider)
                self.spider.logger.info("Scheduler.next_request --> SpiderPriorityQueue: " + request.url +
                                        "|meta:" + str(request.meta) + "|callback:" + str(request.callback) +
                                        "|errback:" + str(request.errback))
        return request

    def has_pending_requests(self):
        return len(self) > 0





