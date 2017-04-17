# -*- coding: utf-8 -*-

import datetime
import random
import string
from pyspark.sql.types import Row

def singleton(cls, *args, **kws):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kws)
        return instances[cls]
    return _singleton


@singleton
class CodeGenerator(object):
    def __init__(self):
        self.sequence = dict()
        self.sequence['rule'] = 100000
        self.sequence['prop'] = 100000
        self.sequence['point'] = 100000
        self.sequence['job'] = 100000

    def get_sequence(self, seq_type):
        if seq_type not in self.sequence.keys():
            raise Exception('输入流水号类型有误：[rule,prop,point]')
        else:
            sequence = self.sequence[seq_type]
            if sequence == 999999:
                self.sequence[seq_type] = 100000
            else:
                self.sequence[seq_type] = sequence+1

            now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            return now + str(sequence)

    def get_random(self, length):
        return ''.join(random.sample(string.ascii_letters + string.digits, length))


    def get_sequence_list(self, seq_type, size):
        l = list()
        for i in range(size):
            l.append(Row(POINT_ID=self.get_sequence(seq_type)))

        return l

if __name__ == '__main__':
    cg = CodeGenerator()

    random_list = cg.get_sequence_list('point',10)


    print(random_list)
