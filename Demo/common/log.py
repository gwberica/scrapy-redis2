# -*- coding: utf-8 -*-
import logging
import logging.config
from .global_value import gol


def init_logger(log_file=None, max_bytes=None, backup_count=None):
    if log_file is None:
        logger = logging.getLogger("")
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)
        logger.setLevel(logging.DEBUG)
        gol.set_value("log", logger)
    else:
        logger = logging.getLogger("")
        formatter = logging.Formatter('[%(asctime)s] [%(process)d:%(thread)d] [%(levelname)s]: %(message)s')
        handle = logging.handlers.RotatingFileHandler(log_file, mode='w', encoding='utf8',
                                                      maxBytes=max_bytes, backupCount=backup_count)
        handle.setFormatter(formatter)
        logger.addHandler(handle)
        logger.setLevel(logging.INFO)
        gol.set_value("log", logger)


def get_logger():
    logger = gol.get_value("log")
    if logger is None:
        init_logger()
    return gol.get_value("log")


def info(msg, *args, **kwargs):
    # for arg in args:
    #     msg = str(msg) + str(arg)
    get_logger().log(logging.INFO, msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    # for arg in args:
    #     msg = str(msg) + str(arg)
    get_logger().log(logging.DEBUG, msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    # for arg in args:
    #     msg = str(msg) + str(arg)
    get_logger().log(logging.WARNING, msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    # for arg in args:
    #     msg = str(msg) + str(arg)
    get_logger().log(logging.ERROR, msg, *args, **kwargs)
