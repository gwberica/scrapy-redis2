from scrapy.crawler import CrawlerProcess

import datetime
import os
import sys

from Demo.spiders.example import ExampleSpider
from common import mongodb_pool
from init import log_dir, base_dir, scrapy_settings, local_host_ip


def start_scrapy(environ='dev', work_id=None, work_name=None):
    LOG_LEVEL_DICT = {
        "dev": "INFO",
        "test": "INFO",
        "rel": "INFO",
    }

    log_level = LOG_LEVEL_DICT[environ]
    if work_id is None:
        work_id = local_host_ip
    print("worker_id: ", work_id)
    if work_name:
        log_file = work_name + "demo" + datetime.datetime.now().strftime(".%Y%m%d.log")
    else:
        log_file = "demo" + datetime.datetime.now().strftime(".%Y%m%d.log")
    log_path = os.path.join(log_dir, log_file)
    scrapy_settings.set('LOG_LEVEL', log_level)
    scrapy_settings.set('LOG_FILE', log_path)
    print("base_dir: ", base_dir)
    print("log_file: ", log_path)
    scrapy_mongodb_db = scrapy_settings.getdict('SCRAPY_MONGODB_DATABASE')[environ]
    scrapy_settings.set('SCRAPY_DATABASE', scrapy_mongodb_db)
    mongodb_cfg = scrapy_settings.getdict('SCRAPY_MONGODB_CONFIG')[environ]
    print("mongodb_cfg:", mongodb_cfg)
    scrapy_settings.set('SCRAPY_WORKER_ID', work_id)
    mongodb_pool.init_mongodb(mongodb_cfg)

    process = CrawlerProcess(scrapy_settings)
    process.crawl(ExampleSpider)

    process.start()


if __name__ == '__main__':
    print("Begin......")
    if len(sys.argv) == 1:
        start_scrapy()
    elif len(sys.argv) >= 3:
        environ = sys.argv[1]
        work_id = sys.argv[2]
        work_name = sys.argv[3]
        start_scrapy(environ, work_id, work_name)
