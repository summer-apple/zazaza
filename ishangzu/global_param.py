# -*- coding: utf-8 -*-
import logging
import logging.config
from configparser import ConfigParser
import os
import sys

try:
    from mysqlconn import MySQLHelper
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from ishangzu.mysqlconn import MySQLHelper


class Logger:

    @staticmethod
    def get_logger():
        logging.config.fileConfig('./conf/logging.conf')
        return logging.getLogger('simpleLogger')

class Global:
    SUCCESS = 0
    FAILURE = 1

    logger = Logger.get_logger()
    configure = ConfigParser()
    configure.read('./conf/spark.conf')



    mysql_helper = MySQLHelper('bigdata')

    # db2_helper = DB2Helper(database=configure.get('db2', 'database'),
    #                        host=configure.get('db2', 'host'),
    #                        port=configure.get('db2', 'port'),
    #                        protocol=configure.get('db2', 'protocol'),
    #                        user=configure.get('db2', 'user'),
    #                        password=configure.get('db2', 'password')
    #                        )


if __name__ == '__main__':
    print(Global.logger)