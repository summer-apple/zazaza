# -*- coding: utf-8 -*-

from configparser import ConfigParser

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import DataFrameReader, DataFrameWriter
import logging
import logging.config
import os
import sys

try:
    from global_param import Global
    from mysqlconn import MySQLHelper
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from ishangzu.global_param import Global
    from ishangzu.mysqlconn import MySQLHelper


class SparkEnvironment(object):
    def __init__(self, app_name='Spark'):

        self.cf = Global.configure

        # spark
        self.conf = (SparkConf()
                     .setAppName(app_name)
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38.jar')
                    )
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)








    def load_from_mysql(self, table):
        return self.sqlctx.read.format('jdbc').options(url='jdbc:mysql://192.168.0.211:3306/bigdata?user=hive&password=hive@2016&characterEncoding=UTF-8',
                                                       dbtable=table,
                                                       driver='com.mysql.jdbc.Driver').load()



if __name__ == '__main__':
    se = SparkEnvironment()
    se.load_from_mysql('page_click_detail').show()