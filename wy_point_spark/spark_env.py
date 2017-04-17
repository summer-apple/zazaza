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
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from product.global_param import Global


class SparkEnvironment(object):
    def __init__(self, app_name='Spark'):

        self.cf = Global.configure

        # spark
        self.conf = (SparkConf()
                     .setAppName(app_name)
                     .set('spark.executor.extraClassPath', self.cf.get('spark', 'extra_class_path'))
                    )
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)
        self.hdfs_base = self.cf.get('spark','hdfs_base')

        # mysql
        # self.mysql_url = cf.get('db', 'url')
        # self.mysql_driver = cf.get('db', 'driver')
        # self.mysql_helper = MySQLHelper(cf.get('db', 'database'), host=cf.get('db', 'host'))





    # def load_from_mysql(self, table):
    #     '''
    #     get dataframe from mysql
    #     :param table:
    #     :return:
    #     '''
    #     return self.sqlctx.read.format('jdbc').options(url=self.mysql_url, dbtable=table,
    #                                                    driver=self.mysql_driver).load()

    def load_from_db2(self, table):
        df = self.sqlctx.read.format("jdbc").options(url=self.cf.get('db2', 'url'),
                                                     dbtable=table,
                                                     user=self.cf.get('db2', 'user'),
                                                     password=self.cf.get('db2', 'password'),
                                                     protocol=self.cf.get('db2', 'protocol'),
                                                     driver=self.cf.get('db2', 'driver')
                                                     ).load()
        return df

    def delete_from_hdfs(self, path):
        if os.system('hadoop fs -test -e ' + path) == 0:  # 256:不存在
            os.system('hadoop fs -rm -r ' + path)

    def save_df(self, path, df):
        self.delete_from_hdfs(path)
        writer = DataFrameWriter(df)
        writer.save(path)

    def read_df(self, path):
        try:
            reader = DataFrameReader(self.sqlctx)
            result = reader.load(path)
        except Exception as e:
            result = None
        return result

    def write_jdbc(self, df, table, mode):
        writer = DataFrameWriter(df)
        url = self.cf.get('db2', 'url')
        properties = {'user': self.cf.get('db2', 'user'),
                      'password': self.cf.get('db2', 'password'),
                      'protocol': self.cf.get('db2', 'protocol'),
                      'driver': self.cf.get('db2', 'driver')
                      }

        writer.jdbc(url,table,mode,properties)


