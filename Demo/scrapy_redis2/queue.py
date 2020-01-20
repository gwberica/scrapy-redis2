from scrapy.utils.reqser import request_to_dict, request_from_dict

from . import picklecompat
from .task_defined import TASK_QUEUE_KEY


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, spider):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        """
        self.server = server
        self.spider = spider
        self.serializer = picklecompat

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        raise NotImplementedError


class SpiderPriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""
    def __init__(self, server, spider):
        self.keys = TASK_QUEUE_KEY['priority_queue']
        super(SpiderPriorityQueue, self).__init__(server, spider)

    def __len__(self):
        r = self.server
        tol = 0
        for i in range(len(self.keys)):
            tol = tol + r.llen(self.keys[i])
        return tol

    def clear(self):
        r = self.server
        for key in self.keys:
            r.delete(key)

    def push(self, request):
        pass

    def pop(self, timeout=0):
        r = self.server
        if timeout > 0:
            data = r.brpop(self.keys, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            for i in range(len(self.keys)):
                data = r.rpop(self.keys[i])
                if data is not None:
                    break
        if data:
            return self._decode_request(data)

