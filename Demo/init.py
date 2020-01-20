import datetime
import os
import sys
import socket
from scrapy.utils.project import get_project_settings


def cur_file_dir():
    """
    获取脚本文件的当前路径
    """
    path = sys.path[0]
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


############################################################################
# 相关路径
#
# base_dir = os.path.join(cur_file_dir(), "..")
base_dir = cur_file_dir()
log_dir = os.path.join(base_dir, "logs")

if not os.path.exists(log_dir):
    os.makedirs(log_dir)
# sys.path.append(base_dir)

local_host_ip = get_host_ip()

LOG_FILE = "car_spider" + datetime.datetime.now().strftime(".%Y%m%d.log")

LOG_FILE_PATH = os.path.join(log_dir, LOG_FILE)
LOG_FILE_MAX_BYTES = 1024 * 1024 * 100
LOG_FILE_BACKUP_COUNT = 5

############################################################################
# settings配置
# 格式：project.settings
#
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'Demo.settings')
scrapy_settings = get_project_settings()
