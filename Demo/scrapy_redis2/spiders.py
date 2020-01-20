from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider
from scrapy.http import Request, Response
from scrapy.spidermiddlewares import httperror
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from twisted.python.failure import Failure
import scrapy
import time
import copy

from . import connection, defaults, request_task
from .queue import SpiderPriorityQueue
from .schedulertask import processed_task_queue


class RedisMixin(object):
    """Mixin class to implement reading urls from a redis queue."""
    redis_encoding = None

    # Redis client placeholder.
    server = None

    callbacks = None

    def start_requests(self):
        """Returns a batch of start requests from redis."""
        req = self.next_requests()
        if req is not None:
            yield req

    def setup_redis(self, crawler=None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.callbacks is None:
            self.callbacks = {}
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings

        if self.redis_encoding is None:
            self.redis_encoding = settings.get('REDIS_ENCODING', defaults.REDIS_ENCODING)

        self.server = connection.from_settings(crawler.settings)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        task_queue = SpiderPriorityQueue(self.server, self)
        request = task_queue.pop(defaults.SCHEDULER_IDLE_BEFORE_CLOSE)
        if request is not None:
            #request = self.check_request_callback(request)
            if request is not None:
                self.logger.info("RedisMixin.next_requests --> SpiderPriorityQueue:%s|callback:%s|errback:%s|meta:%s",
                                 request.url, str(request.callback), str(request.errback), str(request.meta))
                return request
        return None

    def schedule_next_requests(self):
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of redis requests.
        req = self.next_requests()
        if req is not None:
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        # self.schedule_next_requests()
        raise DontCloseSpider

    def _generate_key(self, callback):
        return "%s.%s.%s" % (str(self.__module__), str(self.__class__.__name__), str(callback.__name__))

    def _get_callback(self, key):
        if key:
            return self.callbacks.get(key, None)

    def make_request_from_responses(self, response, url, task_type=None, callback=None, method='GET', headers=None,
                                    body=None, cookies=None, meta=None, encoding='utf-8', priority=0,
                                    errback=None, flags=None, formdata=None,
                                    request_state=request_task.TASK_REQUEST_STATE_CODE_PROCESSED):

        if not isinstance(response, Response):
            raise ValueError("make_request_from_responses: response is required")
        if formdata and 'GET' == method.upper():
            method = 'POST'
        if meta is None:
            meta = {}
        meta['crawler_scheduler'] = copy.copy(response.request.meta['crawler_scheduler'])
        task = request_task.RequestTask(meta['crawler_scheduler'])
        task.callback = None
        task.errback = None
        if callback is not None:
            key = self._generate_key(callback)
            self.callbacks[key] = callback
            task.callback = key
        if errback is not None:
            key = self._generate_key(errback)
            self.callbacks[key] = errback
            task.errback = key
        if task.depth is not None:
            task.depth = task.depth - 1
        task.request_state = request_state
        task.task_type = task_type
        task.create_time = int(time.time())
        task.enqueue_time = None
        if formdata is not None:
            return scrapy.FormRequest(url=url, formdata=formdata, meta=meta, callback=None, method=method,
                                      headers=headers, cookies=cookies, dont_filter=True, errback=None,
                                      encoding=encoding, priority=priority, flags=flags, body=body)
        else:
            return Request(url, meta=meta, callback=None, method=method, headers=headers, cookies=cookies,
                           dont_filter=True, errback=None, encoding=encoding, priority=priority,
                           flags=flags, body=body)

    def update_request_state(self, response):
        if isinstance(response, Response):
            request = response.request
            request_state = request_task.TASK_REQUEST_STATE_CODE_COMPLETED
            self.logger.info('RedisMixin.update_request_state -->completed: ' + str(response))
        elif isinstance(response, Failure):
            request_state = request_task.TASK_REQUEST_STATE_CODE_ERROR
            if response.check(httperror):
                self.logger.error('RedisMixin.update_request_state -->error: ' + repr(response))
                request = response.value.response.request
            elif response.check(DNSLookupError):
                self.logger.error('RedisMixin.update_request_state -->error: ' + repr(response))
                request = response.request
            elif response.check(TCPTimedOutError):
                self.logger.error('RedisMixin.update_request_state -->error: ' + repr(response))
                request = response.request
            else:
                self.logger.error('RedisMixin.update_request_state1 -->unknown: ' + repr(response))
                return
        else:
            self.logger.error('RedisMixin.update_request_state2 -->unknown: ' + repr(response))
            return
        task = request_task.RequestTask.from_request(request)
        task.request_state = request_state
        processed_queue = processed_task_queue(self.server, self)
        processed_queue.push(request)

    def pre_parse(self, response):
        self.logger.info("RedisMixin.pre_parse --> response: %s", response.url)
        task = request_task.RequestTask.from_request(response)
        callback = self.parse
        if task.callback is not None:
            callback = self._get_callback(task.callback)
        reqs = callback(response)
        if reqs is not None:
            yield from reqs
        self.update_request_state(response)

    def pre_parse_errback(self, failure):
        self.logger.info("RedisMixin.pre_parse --> response: %s", str(failure))
        errback = None
        if failure.check(httperror):
            request = failure.value.response.request
            errback = self._get_callback(request_task.RequestTask.get_errback_from_request(request))
        elif failure.check(DNSLookupError):
            request = failure.request
            errback = self._get_callback(request_task.RequestTask.get_errback_from_request(request))
        elif failure.check(TCPTimedOutError):
            request = failure.request
            errback = self._get_callback(request_task.RequestTask.get_errback_from_request(request))
        else:
            self.logger.warning('RedisMixin.pre_parse_errback -->unknown: ' + repr(failure))
        if errback is not None:
            errback(failure)
        self.update_request_state(failure)

    def check_request_callback(self, request):
        task = request_task.RequestTask.from_request(request)
        processed_queue = processed_task_queue(self.server, self)
        if task.callback is not None:
            callback = self._get_callback(task.callback)
            if callback is None:
                self.logger.warning("check_request_callback not find callback: " + request.url + "|" +
                                    str(request.meta['crawler_scheduler']))
                task.request_state = request_task.TASK_REQUEST_STATE_CODE_RETRIED
                processed_queue.push(request)
                return None
        if task.errback is not None:
            errback = self._get_callback(task.errback)
            if errback is None:
                self.logger.warning("check_request_callback not find errback: " + request.url + "|" +
                                    str(request.meta['crawler_scheduler']))
                task.request_state = request_task.TASK_REQUEST_STATE_CODE_RETRIED
                processed_queue.push(request)
                return None
        request.callback = self.pre_parse
        request.errback = self.pre_parse_errback

        return request

    @staticmethod
    def get_task_type(data):
        try:
            task = request_task.RequestTask.from_request(data)
            return task.task_type
        except:
            return None

    @staticmethod
    def get_task_no(data):
        try:
            task = request_task.RequestTask.from_request(data)
            return task.task_no
        except:
            return None

    @staticmethod
    def get_task_params(data):
        try:
            task = request_task.RequestTask.from_request(data)
            return task.params
        except:
            return ''

    @staticmethod
    def set_request_state(request, request_state=request_task.TASK_REQUEST_STATE_CODE_PROCESSED):
        try:
            task = request_task.RequestTask.from_request(request)
            task.request_state = request_state
        except:
            pass


class RedisSpider(RedisMixin, Spider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: False)
        Use SET operations to retrieve messages from the redis queue. If False,
        the messages are retrieve using the LPOP command.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj


class RedisCrawlSpider(RedisMixin, CrawlSpider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: True)
        Use SET operations to retrieve messages from the redis queue.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj
