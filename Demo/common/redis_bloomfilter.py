import six
import math
import json
import hashlib
import random
from .utils import to_bytes


class bloomfilter(object):

    # 调用方法 bloomfilter.__REDIS_BLOCK_MAX_SIZE
    __REDIS_BLOCK_MAX_SIZE = 268435456

    _key = ""

    _block_dict = {}
    _bloomfilter_size = 0      # m
    _hash_func_num = 0         # k
    _elements_max_size = 0     # n
    _elements_size = 0
    _expire_time = 3600

    def __init__(self, r, task_id, num_elements=10000, probability=0.001, ttl=3600):
        self._key = "key:" + task_id
        self._expire_time = ttl
        if r.exists(self._key):
            self.__load_boolfilter(r)
        else:
            self.__init_boolfilter(r, num_elements, probability)

    def __load_boolfilter(self, r):
        '''
        elements_max_size: 成员最大数量
        hash_func_num:     hash函数数量
        bloomfilter_size:  bloomfilter过滤器大小

        elements_size：    当前成员数量
        blocks：           存储在redis的块，由于redis最大只能存储512M，在本bloomfilter买块按照256M计算
        ------------------------------------------------------------------------------------------
        blocks 数据格式：
        {
            'blocknum':1,              在redis中有总共块数，买块最大是256M
            '0': {
                key:      'test001',   块的Key
                block_size:  268435456 块大小(单位为：byte)
            }
        }
        '''
        vals = r.hmget(self._key, 'bloomfilter_size', 'elements_max_size', 'hash_func_num', 'elements_size', 'blocks')

        self._bloomfilter_size = int(vals[0])
        self._elements_max_size = int(vals[1])
        self._hash_func_num = int(vals[2])
        self._elements_size = int(vals[3])
        self._block_dict = json.loads(vals[4])

    def __init_boolfilter(self, r, num_elements, probability):
        self._elements_max_size = num_elements
        self._elements_size = 0

        # k = ln(1/P)/ln2
        self._hash_func_num = int(math.ceil(math.log(1/probability) / math.log(2)))

        # m = ln(1/p)/(ln2**2) * n
        self._bloomfilter_size = int(math.ceil(math.log(1/probability) * num_elements / (math.log(2)**2)))
        blocknum = int(math.ceil(self._bloomfilter_size / bloomfilter.__REDIS_BLOCK_MAX_SIZE))

        self._block_dict = {
            'blocknum': blocknum,
        }
        for i in range(blocknum):
            key = "%s_block%d" % (self._key, i)
            block_size = bloomfilter.__REDIS_BLOCK_MAX_SIZE
            time = self._expire_time + random.randint(30, 100)
            index = "%d" % i
            if i == (blocknum - 1):
                block_size = self._bloomfilter_size - i * bloomfilter.__REDIS_BLOCK_MAX_SIZE
            self._block_dict[index] = {
                'key': key,
                'block_size': block_size
            }
            r.setbit(key, block_size-1, 0)
            r.expire(key, time)

        data = {
            'bloomfilter_size': self._bloomfilter_size,
            'elements_max_size': self._elements_max_size,
            'hash_func_num': self._hash_func_num,
            'elements_size': self._elements_size,
            'blocks': json.dumps(self._block_dict, ensure_ascii=False)
        }
        r.hmset(self._key, data)
        time = self._expire_time + random.randint(0, 30)
        r.expire(self._key, time)

    def __create_hashs(self, str1):
        result = [0] * self._hash_func_num
        k = 0

        while k < self._hash_func_num:
            m = hashlib.md5()
            m.update(to_bytes("%d" % k))
            m.update(to_bytes(str1))

            for i in range(int(m.digest_size / 4)):
                if k < self._hash_func_num:
                    h = 0
                    for j in range(i*4, i*4+4):
                        h = h << 8
                        h = h | (int(m.digest()[j]) & 0xFF)
                    result[k] = h
                    k = k + 1
        return result

    def __set_bit(self, r, hashval):
        val_bit = hashval % self._bloomfilter_size
        for i in range(self._block_dict['blocknum']):
            #print("i:", i, "|blocknum:", self._block_dict['blocknum'])
            index = "%d" % i
            block_info = self._block_dict[index]
            if val_bit < block_info['block_size']:
                r.setbit(block_info['key'], val_bit, 1)
                break
            else:
                val_bit = val_bit - block_info['block_size']

    def __get_bit(self, r, hashval):
        val_bit = hashval % self._bloomfilter_size
        val = 0
        for i in range(self._block_dict['blocknum']):
            index = "%d" % i
            block_info = self._block_dict[index]
            if val_bit < block_info['block_size']:
                val = r.getbit(block_info['key'], val_bit)
                break
            else:
                val_bit = val_bit - block_info['block_size']
        return val

    def add(self, r, data):
        hashes = self.__create_hashs(data)
        for hashval in hashes:
            self.__set_bit(r, hashval)

    def contains(self, r, data):
        hashes = self.__create_hashs(data)
        for hashval in hashes:
            if self.__get_bit(r, hashval) == 0:
                return False
        return True

    def clear(self, r):
        for i in range(self._block_dict['blocknum']):
            index = "%d" % i
            block_info = self._block_dict[index]
            r.delete(block_info['key'])
        r.delete(self._key)













