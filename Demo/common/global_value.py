# -*- coding: utf-8 -*-
from .singleton import Singleton


class GlobalVariable(Singleton):

    __global_dict = None

    def __init__(self):
        super(GlobalVariable, self).__init__()
        self.__global_dict = {}

    def set_value(self, key, value):
        self.__global_dict[key] = value

    def get_value(self, key, defValue=None):
        try:
            return self.__global_dict[key]
        except KeyError:
            return defValue


gol = GlobalVariable()

