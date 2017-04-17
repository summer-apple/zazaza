# -*- coding: utf-8 -*-

import os
import sys
import time

try:
    from za.point_rule import PointEngine
    from code_generator import CodeGenerator
    from global_param import Global
    from exceptions import *
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from product.point_rule import PointEngine
    from product.code_generator import CodeGenerator
    from product.global_param import Global
    from product.exceptions import *

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)

class Runner:

    def __init__(self, data_date):
        self.data_date = data_date
        self.MAX_FAIL_TIMES = 1
        self.point_engine = PointEngine()
        self.cg = CodeGenerator()


    def _get_rule_by_id(self, rule_id, special_flag=0):
        return self.point_engine.get_rule_by_id(rule_id, special_flag)

    def _get_rules_by_ids(self,ids):
        rules = list()
        for id in ids:
            rules.append(self._get_rule_by_id(*id))
        return rules


    def _run_rule(self, rule, sum_to_account=False):
        # 重复执行检测 !!! 不能用批次号来判断是否已执行过 因为此时批次号未知
        if self._is_already_executed(rule.rule_id, rule.special_flag):
            Global.logger.warn('数据日期：%s 规则已经成功执行过， 请勿重复执行！：%s' % (self.data_date, str(rule)))
            return
            # raise Exception('规则：%s 数据日期：%s已经成功执行过， 请勿重复执行！' % (str(rule), data_date))

        # 生成批次号
        batch_id = self.cg.get_random(10)
        success_flag = False
        for i in range(self.MAX_FAIL_TIMES):
            # 生成流水水号
            job_id = self.cg.get_sequence('job')
            try:
                Global.logger.info('START JOB_ID:%s ----- %s' % (job_id, str(rule)))
                # 开始执行规则
                self._start_job(job_id, batch_id, rule.rule_id, rule.special_flag)
                # 执行规则
                if sum_to_account:
                    result = self.point_engine.execute_and_sum(rule, self.data_date, job_id)
                    # 更新规则执行结果
                    self._update_result(job_id, result)
                    if result == Global.SUCCESS:
                        success_flag = True
                        break
                else:
                    # 此处未汇总到积分账户表
                    result = self.point_engine.execute(rule, self.data_date, job_id)
                    return job_id


            except Exception as e:
                # 更新规则执行结果
                self._update_result(job_id, Global.FAILURE)
                Global.logger.error('ERROR %s' % e)
                continue  # 再次尝试

        if not success_flag:
            raise MaxFailureError('规则：%s 以达到最大失败次数%s' % (str(rule), self.MAX_FAIL_TIMES))

    def run_by_cycle(self, rule_cycle):
        """
        根据规则周期执行规则
        :param rule_cycle:
        :return:
        """
        # 按周期获取规则列表
        rules = self.point_engine.get_rules_by_cycle(rule_cycle)
        self.run_batch(rules)

    def run_single(self, rule_id, special_flag=0):
        """
        执行单条规则
        :param rule_id:
        :param special_flag:
        :return:
        """

        # 清空暂存表
        self.point_engine.clear_point_detail_tmp()
        self.point_engine.clear_point_account_tmp()

        rule = self._get_rule_by_id(rule_id, special_flag)
        self._run_rule(rule, sum_to_account=True)

    def run_batch(self, rules):
        """
        批量执行规则
        :param rules:规则list
        :return:
        """
        if rules is None or len(rules) == 0:
            Global.logger.info('run_batch--没有要执行的规则')
            return

        # 清空暂存表
        self.point_engine.clear_point_detail_tmp()
        self.point_engine.clear_point_account_tmp()

        if isinstance(rules[0], tuple):
            rules = self._get_rules_by_ids(rules)

        run_results = list()
        for rule in rules:
            result = self._run_rule(rule, sum_to_account=False)
            run_results.append(result)

            time.sleep(Global.RUN_INTERVAL)

        # 汇总
        sum_result = self.point_engine.sum_detail_to_account()
        if sum_result == Global.SUCCESS and len(run_results)>0:
            for job_id in run_results:
                self._update_result(job_id, '0')

    def _is_already_executed(self, rule_id, special_flag):
        """
        重复执行检测 !!! 不能用批次号来判断是否已执行过 因为此时批次号未知
        :param rule_id:
        :param special_flag:
        :return:
        """
        check_sql = "SELECT count(1) FROM T_POINT_SPARK_JOB WHERE RULE_ID=? AND SPECIAL_FLAG=? AND DATA_DATE=? AND RESULT='0'"
        count = Global.db2_helper.fetchone(check_sql, [rule_id, special_flag, self.data_date])[0]

        if count > 0:
            # 该日期已成功执行过此规则
            return True
        else:
            return False

    def _start_job(self, job_id, batch_id, rule_id, special_flag):
        # 获取重复次数
        repetition = self._get_repetition(batch_id)
        start_sql = "insert into T_POINT_SPARK_JOB(JOB_ID, BATCH_ID, RULE_ID, SPECIAL_FLAG, REPETITION, DATA_DATE, START_TIME, RESULT) values(?,?,?,?,?,?,CURRENT_TIMESTAMP,'1')"
        Global.db2_helper.execute(start_sql, [job_id, batch_id, rule_id, special_flag, repetition, self.data_date])

    def _update_result(self, job_id, result):
        update_sql = "update T_POINT_SPARK_JOB set RESULT=? , END_TIME=CURRENT_TIMESTAMP where JOB_ID=?"
        Global.db2_helper.execute(update_sql, [result, job_id])

    def _get_repetition(self, batch_id):
        # 获取当前规则已经重复执行的次数
        repetition_sql = "SELECT max(REPETITION) FROM T_POINT_SPARK_JOB WHERE BATCH_ID=?"

        repetition_result = Global.db2_helper.fetchone(repetition_sql, [batch_id, ])  # [None]  or [2]

        if repetition_result[0] is not None:
            repetition = repetition_result[0] + 1
        else:
            repetition = 1
        return repetition

if __name__ == '__main__':

    runner = Runner(data_date='20170409')

    #runner.run_single('gr_hq_09')
    runner.run_single('gr_qy_04')
    Global.db2_helper.execute("TRUNCATE TABLE  T_POINT_SPARK_JOB IMMEDIATE")
    #runner.run_by_cycle(1)


    # rules = [
    #     ('gr_dk_01','0'),
    #     ('gr_dk_02', '0'),
    #     ('gr_dk_03', '0')
    # ]
    # runner.run_batch(rules)