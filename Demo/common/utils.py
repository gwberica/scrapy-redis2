# -*- coding: utf-8 -*-

from urllib.parse import quote
from urllib.parse import unquote
#
#


# 判断队列是否有None
def check_list(l):
  for val in l:
    if val is None:
      return False
  return True


def is_string(str1):
  return type(str1) == type('string')


def string_empty(str1):
  if not is_string(str1):
    return True
  return str1.strip(' \t\r\n') != ''


def to_str(bytes_or_str, encoding='utf-8'):
  if isinstance(bytes_or_str, bytes):
    value = bytes_or_str.decode(encoding)
  else:
    value = bytes_or_str
  return value


def to_bytes(bytes_or_str, encoding='utf-8'):
  if isinstance(bytes_or_str, str):
    value = bytes_or_str.encode(encoding)
  else:
    value = bytes_or_str
  return value


def urlencode(s):
  return quote(s, 'utf-8')


def urldecode(s):
  return unquote(s, 'utf-8')
