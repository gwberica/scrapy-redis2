from . import picklecompat
from .task_defined import TASK_QUEUE_KEY
from scrapy.utils.reqser import request_to_dict, request_from_dict


class processed_task_queue(object):

    def __init__(self, server, spider):
        self.server = server
        self.spider = spider
        self.serializer = picklecompat
        self.key = TASK_QUEUE_KEY['processed_queue']

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def push(self, request):
        r = self.server
        r.lpush(self.key, self._encode_request(request))

    def clear(self):
        r = self.server
        r.delete(self.key)

    def __len__(self):
        r = self.server
        return r.llen(self.key)




