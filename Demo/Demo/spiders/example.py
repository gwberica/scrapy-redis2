# -*- coding: utf-8 -*-
from scrapy_redis2.spiders import RedisSpider


class ExampleSpider(RedisSpider):
    name = 'example'
    allowed_domains = ['example.com']
    start_urls = ['http://example.com/']

    def parse(self, response):
        # 获取任务类型
        task_type = RedisSpider.get_task_type(response)
        # 获取传递参数
        params = RedisSpider.get_task_params(response)
        # 获取任务编号------>> 唯一
        task_no = RedisSpider.get_task_no(response)
        task_type = task_type.strip(' \r\n\t')

        yield from self.test(response)

    def test(self, response):
        pass
