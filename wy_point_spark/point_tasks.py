# -*- coding: utf-8 -*-
import pydevd
from apscheduler.schedulers.background import BlockingScheduler,BackgroundScheduler
import os
import sys
import time
import datetime
try:
    from run_rule import Runner
    from global_param import Global
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from product.run_rule import Runner
    from product.global_param import Global

pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)

class PointTask:

    def task_func(self):

        sql = "select * from T_POINT_DATA_PREPARE WHERE PREPARED='0' AND CALCULATED='1'"
        runner = Runner(data_date=None)


        while True:
            now_clock = datetime.datetime.now().hour
            if 8 <= now_clock <= 20:
                time.sleep((25-now_clock)*60*60)

            result = Global.db2_helper.fetchall(sql)
            if len(result) == 0:  # not ready
                Global.logger.warn('数据抽取未完成...等待10分钟！')
                time.sleep(600)
                continue
            else:

                for r in result:  # [DATA_DT,PREPARED,CALCULATED]
                    # 数据库准备日期是1号，Runner的日期为2号
                    one_day_later = (datetime.datetime.strptime(r[0], '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
                    runner.data_date = one_day_later
                    try:
                        self.run_each_cycle(runner)
                        Global.db2_helper.execute("UPDATE T_POINT_DATA_PREPARE SET CALCULATED='2' WHERE DATA_DATE='%s'" % r[0])
                    except Exception as e:
                        Global.db2_helper.execute("UPDATE T_POINT_DATA_PREPARE SET CALCULATED='3' WHERE DATA_DATE='%s'" % r[0])

                    # 如果两天前的数据准备成功，3号凌晨计算1号的数据
                    two_day_before = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')  # 计算两天前的
                    if r[0] == two_day_before:
                        time.sleep((25 - datetime.datetime.now().hour) * 60 * 60)




    def run_each_cycle(self,runner):
        """
        任务调度. 按天、周、月。。。顺序执行
        :return:
        """

        data_date = datetime.datetime.strptime(runner.data_date,'%Y%m%d')

        day = data_date.day
        weekday = data_date.weekday()
        month = data_date.month

        Global.logger.info('today：%s' % datetime.datetime.now().strftime('%Y-%m-%d'))
        Global.logger.info('数据日期%s(runner param) true data_date is one day before    定时任务开始\n' % runner.data_date)

        # 每天先更新关系表
        # point_engine.update_relation()
        # point_engine.logger.info('RELATION TABLE UPDATED!')




        # day 每天
        runner.run_by_cycle(1)

        # week 周yi
        if weekday == 1:
            runner.run_by_cycle(2)

        # month 每月1号
        if day == 1:
            runner.run_by_cycle(3)

        # season
        if day == 1 and month in [1, 4, 7, 10]:
            runner.run_by_cycle(4)

        # half_year
        if day == 1 and month in [1, 7]:
            runner.run_by_cycle(5)

        # year
        if day == 1 and month == 1:
            runner.run_by_cycle(6)

            Global.logger.info('\n\n\n数据日期%s定时任务结束\n' % runner.data_date)

    def task(self):
        """
        !!!!this function is useless don't run it!!!!
        Parameters:
            year (int|str) – 4-digit year
            month (int|str) – month (1-12)
            day (int|str) – day of the (1-31)
            week (int|str) – ISO week (1-53)
            day_of_week (int|str) – number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
            hour (int|str) – hour (0-23)
            minute (int|str) – minute (0-59)
            second (int|str) – second (0-59)
            start_date (datetime|str) – earliest possible date/time to trigger on (inclusive)
            end_date (datetime|str) – latest possible date/time to trigger on (inclusive)
            timezone (datetime.tzinfo|str) – time zone to use for the date/time calculations (defaults to scheduler timezone)
        :return:
        """
        scheduler = BlockingScheduler()
        #scheduler.add_job(self.task_func, trigger='cron', day='*/1', hour='1')
        scheduler.add_job(self.task_func, trigger='cron', minute='*/5')
        #scheduler.add_job(func, 'date', run_date='2016-10-25 13:51:30')
        try:
            scheduler.start()
        except Exception as e:
            # TODO 执行错误的处理方案
            Global.logger.error('定时任务错误:%s' % e)
            scheduler.shutdown()







if __name__ == '__main__':
    task = PointTask()
    task.task_func()