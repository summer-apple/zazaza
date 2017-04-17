# -*- coding: utf-8 -*-

import pydevd
from pyspark.sql.types import Row
import datetime
import calendar as cal
import sys, os
import decimal
import uuid
import time

from pyspark.sql.readwriter import DataFrameWriter, DataFrameReader

try:
    from spark_env import SparkEnvironment
    from code_generator import CodeGenerator
    from exceptions import *
    from global_param import Global
except ImportError:
    sys.path.append(os.path.abspath('../'))
    from product.spark_env import SparkEnvironment
    from product.code_generator import CodeGenerator
    from product.exceptions import *
    from product.global_param import Global


#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)

class Rule:
    def __init__(self, rule_id, special_flag, rule_name,source_table, cust_pk, point_rule, rule_cycle, rule_state, version, desp, create_time, props=None):
        self.rule_id = rule_id
        self.special_flag = special_flag
        self.rule_name = rule_name
        self.source_table = source_table
        self.cust_pk = cust_pk
        self.point_rule = point_rule

        self.rule_cycle = rule_cycle
        self.rule_state = rule_state
        self.version = version
        self.desp = desp
        self.create_time = create_time

        self.props = props

    def __str__(self):
        return '规则ID:%s 规则名:%s)' % (self.rule_id, self.rule_name)


class Prop:
    def __init__(self, prop_id, rule_id, special_flag, point_flag, operation, date_key, prop_key, compare, prop_value, prop_state, desp):
        self.prop_id = prop_id
        self.rule_id = rule_id
        self.special_flag = special_flag
        self.point_flag = point_flag
        self.operation = operation
        self.date_key = date_key
        self.prop_key = prop_key
        self.compare = compare
        self.prop_value = prop_value
        self.prop_state = prop_state
        self.desp = desp



class PointEngine:
    def __init__(self):
        self.spark = SparkEnvironment(app_name='PointEngine')
        self.cg = CodeGenerator()


    # ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 获取规则方法 START ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓

    def get_rule_by_id(self, rule_id, special_flag=0):
        print('rule_id:%s' % rule_id)
        rule_result = Global.db2_helper.fetchone("select * from t_POINT_RULE where RULE_ID='%s' and SPECIAL_FLAG=%s" % (rule_id, special_flag))
        Global.logger.debug(rule_result)
        if len(rule_result) == 0 or rule_result is None:
            return None
        else:

            prop_result = Global.db2_helper.fetchall("select * from T_POINT_PROP where RULE_ID='%s' and SPECIAL_FLAG=%s and PROP_STATE=0" % (rule_id, special_flag))
            Global.logger.debug(prop_result)
            props = []
            for p in prop_result:
                prop = Prop(*p)
                props.append(prop)

            # 创建规则对象
            rule = Rule(*rule_result, props=props)
            return rule

    def get_rules_by_cycle(self, rule_cycle):
        '''
        :param rule_cycle:
                            1:day
                            2:week
                            3:month
                            4:season
                            5:half_year
                            6:year
        :return:
        '''


        # TODO STATE=1 正在运行中 如何处理？
        sql_rule = "select * from T_POINT_RULE where RULE_CYCLE=%s and RULE_STATE='0' order by RULE_ID ASC, SPECIAL_FLAG DESC" % rule_cycle
        rule_result = Global.db2_helper.fetchall(sql_rule)

        if len(rule_result) == 0:
            return None
        else:
            rules = []
            for r in rule_result:
                Global.logger.debug('rule:%s' % r)
                rule_id = r[0]
                special_flag = r[1]
                sql_prop = "select * from T_POINT_PROP where RULE_ID=? and SPECIAL_FLAG=? and PROP_STATE=0"
                prop_result = Global.db2_helper.fetchall(sql_prop, [rule_id, special_flag])

                if len(prop_result) < 1:
                    Global.logger.warn('规则没有属性，将直接跳过。。。%s' % str(r))
                    continue
                props = []
                for p in prop_result:
                    Global.logger.debug('prop:%s' % p)
                    prop = Prop(*p)
                    props.append(prop)

                rule = Rule(*r, props=props)
                Global.logger.info(str(rule))
                rules.append(rule)

            return rules

    # ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ 获取规则方法 END ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑

    # ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 执行规则方法 START ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓

    def execute(self, rule, data_date, job_id):
        """
        执行规则，结果存入积分明细暂存表（T_POINT_DETAIL_TMP）
        :param rule:规则
        :param data_date: 指定执行某一天的数据
        :param job_id: spark 任务id
        :return:
        """



        # !!! 此处仅对存款类规则进行处理，周一从t_point_dep_01,周二从02 依次类推
        if rule.rule_cycle == '1' and rule.rule_id in ['gr_ck_01', 'gr_ck_02']:
            true_data_date = datetime.datetime.strptime(data_date, '%Y%m%d') - datetime.timedelta(days=1)
            if true_data_date.weekday() == 6:
                week = '01'
            else:
                week = '%02d' % (true_data_date.weekday() + 2)
            rule.source_table = 'T_POINT_DEP_%s' % week




        df_origin = self.spark.load_from_db2(rule.source_table)


        point_prop = None
        prop_sum_avg_count_list = list()


        # 遍历compare
        for prop in rule.props:

            # 找出计分的属性
            if prop.point_flag == '1':
                point_prop = prop

            if prop.operation in ['sum','count','avg']:
                prop_sum_avg_count_list.append(prop)
                continue

            condition = "trim(%s)%s'%s'" % (prop.prop_key, prop.compare, prop.prop_value)  # 过滤条件
            Global.logger.debug('condition:%s' % condition)

            # 如果属性中时间字段不为空，则进行过滤 ---> 指定某个日期之前周期的数据
            if prop.date_key is not None and len(prop.date_key) > 0:
                date_limit_condition = self._date_limit(rule.rule_cycle, prop.date_key, data_date)
                df_origin = df_origin.filter(date_limit_condition)

            # 调用过滤方法，针对属性进行过滤
            df_origin = self._prop_filter(rule, prop, df_origin, condition)

            Global.logger.debug('-------------------------One Prop Finish--------------------------')



        for prop in prop_sum_avg_count_list:
            condition = 'VAL' + prop.compare + prop.prop_value
            Global.logger.debug('condition:%s' % condition)

            # 如果属性中时间字段不为空，则进行过滤 ---> 指定某个日期之前周期的数据
            if prop.date_key is not None and len(prop.date_key) > 0:
                date_limit_condition = self._date_limit(rule.rule_cycle, prop.date_key, data_date)
                df_origin = df_origin.filter(date_limit_condition)

            # 调用过滤方法，针对属性进行过滤
            df_origin = self._prop_filter(rule, prop, df_origin, condition)


            Global.logger.debug('-------------------------One Prop Finish--------------------------')





        if point_prop is None:
            raise Exception('规则ID:%s 未找到计分属性！' % rule.rule_id)


        selection = [rule.cust_pk, point_prop.prop_key]
        df = df_origin.selectExpr(*selection)

        # 根据主键group by并计数
        if point_prop.operation == 'count':
            df = df.groupBy(selection[0]).count()
        if point_prop.operation == 'sum':
            df = df.groupBy(selection[0]).sum(df.columns[1])
        if point_prop.operation == 'avg':
            df = df.groupBy(selection[0]).avg(df.columns[1])


        # 最终group by后
        condition = 'VAL' + prop.compare + prop.prop_value
        df = df.withColumnRenamed(df.columns[1], 'VAL').filter(condition)





        got_point_df = df.withColumnRenamed(df.columns[0],'PK')


        # 计算积分
        try:
            point_history_df = self._calculate_point(rule, got_point_df)
        except RDDEmptyError as e:
            return Global.SUCCESS
        except Exception as e:
            return Global.FAILURE

        # 积分计算结果存入流水暂存表
        try:
            self._save_point_detail_tmp(rule, point_history_df, job_id)
            return Global.SUCCESS
        except RDDEmptyError as e:
            return Global.SUCCESS
        except SQLError as e:
            return Global.FAILURE

    def sum_detail_to_account(self):
        """
        执行完多个规则之后，结果汇总入积分账户表（T_POINT_ACCOUNT)
        :return:
        """
        try:
            self._save_account_tmp()
            self._account_tmp_to_real()
        except SQLError as e:
            return Global.FAILURE
        except RDDEmptyError as e:
            return Global.SUCCESS

        return Global.SUCCESS

    def execute_and_sum(self, rule, data_date, job_id):
        """
        执行单个规则，结果汇总入积分账户表（T_POINT_ACCOUNT)
        :param rule:规则
        :param data_date: 指定执行某一天的数据
        :param job_id: spark 任务id
        :return:
        """
        r1 = self.execute(rule, data_date, job_id)

        if r1 == 1:
            return Global.FAILURE
        else:
            try:
                self._save_account_tmp()
                self._account_tmp_to_real()
            except SQLError as e:
                return Global.FAILURE
            except RDDEmptyError as e:
                return Global.SUCCESS

            return Global.SUCCESS

    # ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ 执行规则方法 END ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑

    # ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 日期、规则过滤,积分计算方法 START ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓

    def _calculate_point(self, rule, got_point_df):

        '''
        解析积分规则，计算积分
        T_POINT_RELATION_ACC --> ACC_NO CARD_NO CUST_NO
        T_POINT_RELATION_CUST --> CUST_NO CUST_ID
        :param cust_pk:
        :param point_rule:  10000,1,15   每10000积1分，15分封顶
        :param got_point_df:
        :return:
        '''

        Global.logger.debug('point_rule:%s' % rule.point_rule)
        pr = rule.point_rule.split(',')

        fmt = Global.configure.get('param','point_fmt')
        if fmt == 'float':
            fmt = float
        else:
            fmt = int

        per = fmt(pr[0])
        p = fmt(pr[1])
        top = fmt(pr[2])

        def m(line):
            val = fmt(line['VAL'])

            point = fmt(val / per) * p

            if top != 0 and point > top:
                point = top

            return Row(PK=line['PK'], VAL=line['VAL'], POINT=int(point))

        try:
            pk_val_point_df = self.spark.sqlctx.createDataFrame(got_point_df.map(m)).filter('POINT!=0') \
                .withColumnRenamed('PK', 'CUST_NO')
        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:该规则没有用户获得积分')
                raise RDDEmptyError('RDD is empty:该规则没有用户获得积分')
            else:
                Global.logger.error(e)
                raise Exception(e)

        return pk_val_point_df

    def _date_limit_with_dash(self, rule_cycle, date_key, data_date):
        '''
        1:day
        2:week
        3:month
        4:season
        5:half_year
        6:year
        :param rule_cycle:
        :param date_key:
        :param data_date: 传入指定日期执行规则，若不传，则按当天日期来
        :return:
        '''

        today = datetime.datetime.now()
        if data_date is not None:
         today = datetime.datetime.strptime(data_date, '%Y-%m-%d')


        if rule_cycle == '1':  # -------------------------DAY------------------------
            yestoday = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 计算两天前的
            date_limit_condition = date_key + "='" + yestoday + "'"
        else:
            if rule_cycle == '2':  # -------------------------WEEK------------------------
                weekday = today.weekday()  # 0-6
                start = (today - datetime.timedelta(days=weekday + 7)).strftime('%Y-%m-%d')
                end = (today - datetime.timedelta(days=weekday + 1)).strftime('%Y-%m-%d')
            elif rule_cycle == '3':  # -------------------------MONTH------------------------
                year = today.year
                month = today.month - 1 # 上个月
                month_range = cal.monthrange(year, month)   # (0,29)
                start = str(year) + '-' + ('%02d' % month) + '-01'
                end = str(year) + '-' + ('%02d' % month) + '-' + str(month_range[1])
            elif rule_cycle == '4':  # -------------------------SEASON------------------------
                year = today.year
                month = today.month
                if month>=1 and month<=3:
                    start = str(year) +'-01-01'
                    end = str(year) +'-03-31'
                elif month>=4 and month<=6:
                    start = str(year) + '-04-01'
                    end = str(year) + '-06-30'
                elif month>=7 and month<=9:
                    start = str(year) + '-07-01'
                    end = str(year) + '-09-30'
                else:
                    start = str(year) + '-10-01'
                    end = str(year) + '-12-31'

            elif rule_cycle == '5':  # -------------------------HALF YEAR------------------------
                month_now = today.month
                if month_now < 7:  # 处于上半年 计算前一年下半年数据
                    year = today.year - 1
                    start = str(year) + '-07-01'
                    end = str(year) + '-12-31'
                else:  # 处于下半年，计算今年上半年数据
                    year = today.year
                    start = str(year) + '-01-01'
                    end = str(year) + '-06-30'
            elif rule_cycle == '6':  # -------------------------YEAR------------------------
                year = today.year - 1
                start = str(year) + '-01-01'
                end = str(year) + '-12-31'

            Global.logger.info('date limit: %s -- %s' % (start, end))
            date_limit_condition = date_key + ">='" + start + "' and " + date_key + "<='" + end + "'"
        Global.logger.debug('date_limit_condition:%s' % date_limit_condition)

        return date_limit_condition

    def _date_limit(self, rule_cycle, date_key, data_date):
        '''
        1:day
        2:week
        3:month
        4:season
        5:half_year
        6:year
        :param rule_cycle:
        :param date_key:
        :param data_date: 传入指定日期执行规则，若不传，则按当天日期来
        :return:
        '''

        today = datetime.datetime.now()
        if data_date is not None:
            today = datetime.datetime.strptime(data_date, '%Y%m%d')

        if rule_cycle == '1':  # -------------------------DAY------------------------
            yesterday = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')  # 计算两天前的
            date_limit_condition = date_key + "='" + yesterday + "'"
        else:
            if rule_cycle == '2':  # -------------------------WEEK------------------------
                weekday = today.weekday()  # 0-6
                start = (today - datetime.timedelta(days=weekday + 7)).strftime('%Y%m%d')
                end = (today - datetime.timedelta(days=weekday + 1)).strftime('%Y%m%d')
            elif rule_cycle == '3':  # -------------------------MONTH------------------------
                year = today.year
                month = today.month - 1  # 上个月
                month_range = cal.monthrange(year, month)  # (0,29)
                start = str(year) + ('%02d' % month) + '01'
                end = str(year) + ('%02d' % month) + str(month_range[1])
            elif rule_cycle == '4':  # -------------------------SEASON------------------------
                year = today.year
                month = today.month
                if month >= 1 and month <= 3:
                    start = str(year) + '0101'
                    end = str(year) + '0331'
                elif month >= 4 and month <= 6:
                    start = str(year) + '0401'
                    end = str(year) + '0630'
                elif month >= 7 and month <= 9:
                    start = str(year) + '0701'
                    end = str(year) + '0930'
                else:
                    start = str(year) + '1001'
                    end = str(year) + '1231'

            elif rule_cycle == '5':  # -------------------------HALF YEAR------------------------
                month_now = today.month
                if month_now < 7:  # 处于上半年 计算前一年下半年数据
                    year = today.year - 1
                    start = str(year) + '0701'
                    end = str(year) + '1231'
                else:  # 处于下半年，计算今年上半年数据
                    year = today.year
                    start = str(year) + '0101'
                    end = str(year) + '0630'
            elif rule_cycle == '6':  # -------------------------YEAR------------------------
                year = today.year - 1
                start = str(year) + '0101'
                end = str(year) + '1231'

            Global.logger.info('date limit: %s -- %s' % (start, end))
            date_limit_condition = date_key + ">='" + start + "' and " + date_key + "<='" + end + "'"
        Global.logger.debug('date_limit_condition:%s' % date_limit_condition)

        return date_limit_condition



    def _prop_filter(self, rule, prop, df_origin, condition):
        '''
        根据属性中的条件，过滤相应的df
        :param prop: 属性对象
        :param df_origin: 原始df
        :param selection: select的字段
        :param condition: 选择条件
        :return:
        '''


        def xxx(operation):
            # 选出主键和要计数的字段
            selection = [rule.cust_pk, prop.prop_key]
            df = df_origin.selectExpr(*selection)

            # 根据主键group by并计数
            if operation == 'count':
                df = df.groupBy(selection[0]).count()
            if operation == 'sum':
                df = df.groupBy(selection[0]).sum(df.columns[1])
            if operation == 'avg':
                df = df.groupBy(selection[0]).avg(df.columns[1])

            df = df.withColumnRenamed(df.columns[1], 'VAL').filter(condition)

            df = df.select(rule.cust_pk).join(df_origin, rule.cust_pk, 'left_outer')

            return df

        # 比较
        if prop.operation == 'compare':
            # 先选择字段 再进行过滤
            df = df_origin.filter(condition)

        # 包含
        elif prop.operation == 'in':
            temp = None
            for i in prop.prop_value.split(','):
                condition = "trim(%s)%s'%s'" % (prop.prop_key, prop.compare, i)  # 过滤条件
                Global.logger.debug('operation_in_condition:%s' % condition)
                if temp is None:
                    temp = df_origin.filter(condition)
                else:
                    temp = temp.unionAll(df_origin.filter(condition))
            df = temp


        # 计数或求和或求平均
        elif prop.operation in ['count','sum','avg']:
            df = xxx(prop.operation)


        else:
            raise Exception('操作类型（%s）无法识别' % prop.operation)

        return df



    def _prop_filter_old(self, prop, df_origin, selection, condition):
        '''
        根据属性中的条件，过滤相应的df
        :param prop: 属性对象
        :param df_origin: 原始df
        :param selection: select的字段
        :param condition: 选择条件
        :return:
        '''
        # 比较

        if prop.operation == 'compare':
            # 先选择字段 再进行过滤
            df = df_origin.selectExpr(*selection).withColumnRenamed(selection[0], 'PK') \
                .withColumnRenamed(selection[1], 'VAL') \
                .filter(condition)

        # 计数或求和
        elif prop.operation == 'count':
            # 选出主键和要计数的字段
            df = df_origin.selectExpr(*selection)
            # 根据主键group by
            df = df.groupBy(selection[0]).count()
            df = df.withColumnRenamed(selection[0], 'PK').withColumnRenamed(df.columns[1], 'VAL').filter(condition)

        elif prop.operation == 'sum':
            # 选出主键和要计数的字段
            df = df_origin.selectExpr(*selection)
            # 根据主键group by
            df = df.groupBy(selection[0]).sum(df.columns[1])
            df = df.withColumnRenamed(selection[0], 'PK').withColumnRenamed(df.columns[1], 'VAL').filter(condition)
        elif prop.operation == 'avg':
            # 选出主键和要计数的字段
            df = df_origin.selectExpr(*selection)
            # 根据主键group by
            df = df.groupBy(selection[0]).avg(df.columns[1])
            df = df.withColumnRenamed(selection[0], 'PK').withColumnRenamed(df.columns[1], 'VAL').filter(
                condition)
        # 包含
        elif prop.operation == 'in':
            temp = None
            df = df_origin.selectExpr(*selection).withColumnRenamed(selection[0], 'PK').withColumnRenamed(selection[1], 'VAL')
            for i in prop.prop_value.split(','):
                condition = 'VAL' + prop.compare + i
                Global.logger.debug('operation_in_condition:%s' % condition)
                if temp is None:
                    temp = df.filter(condition)
                else:
                    temp = temp.unionAll(df.filter(condition))
            df = temp.filter(condition)

        # 连续型
        elif prop.operation == 'continue':
            # df_continue 连续表
            df = self.spark.load_from_db2('t_POINT_CONTINUE')
            df = df.selectExpr('trim(SOURCE_TABLE)', 'trim(CONTINUE_KEY)', 'CUST_PK', 'CURRENT_VALUE')

            # source_table 和 continue_key 和 update_time 是联合主键
            df = df.filter(df['trim(SOURCE_TABLE)'] == prop.source_table).filter(df['trim(CONTINUE_KEY)'] == prop.prop_key)
            df = df.select('CUST_PK', 'CURRENT_VALUE').withColumnRenamed('CUST_PK', 'PK') \
                .withColumnRenamed('CURRENT_VALUE', 'VAL').filter(condition)
        else:
            raise Exception('操作类型（%s）无法识别' % prop.operation)

        return df

    # ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ 日期、规则 过滤,积分计算方法 END ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑

    # ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 保存、汇总 积分流水方法 START ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓

    def _save_point_detail_tmp(self, rule, df, job_id):
        """
        将积分流水保存到T_POINT_DETAIL_TMP
        :param rule: 规则
        :param df: 积分df
        :param job_id: 任务id
        :return:
        """

        rule_name = rule.rule_name

        # 去除黑名单用户(黑名单用户都不会再增加积分)
        black_df = self.spark.load_from_db2('T_POINT_BLACK').select('CUST_NO')
        sub_df = df.select('CUST_NO').subtract(black_df.select('CUST_NO'))
        df = sub_df.join(df, 'CUST_NO', 'left')

        def m(x):
            # 去除客户号不是10开头的数据
            cust_no = x['CUST_NO']
            if cust_no[:2] != '10':
                cust_no = ''

            point = x['POINT']
            if point >= 0:
                points_type = '0004'
            else:
                points_type = '1005'

            point_id = str(uuid.uuid1())
            now = datetime.datetime.now()
            occur_date = now.strftime('%Y%m%d')
            occur_time = now.strftime('%H%M%S')
            timestamp = str(time.time()).replace('.','')
            return Row(SYS_SERIAL_NO=point_id, OCCUR_DATE=occur_date, OCCUR_TIME=occur_time, POINTS_TYPE=points_type,
                       CUST_NO=cust_no, POINTS_VAL=point, STATUS='0', TIMESTAMP=timestamp, REMARK=rule_name, BAK2=job_id)



        try:
            point_history_df = self.spark.sqlctx.createDataFrame(df.map(m)).filter("CUST_NO!=''")  # 去除客户号不是10开头的数据
            self.spark.write_jdbc(point_history_df, 'T_POINT_DETAIL_TMP', 'append')
            Global.logger.info('积分流水保存(T_POINT_DETAIL_TMP)--成功')

        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有积分流水信息')
                raise RDDEmptyError('RDD is empty:没有积分流水信息')
            else:
                Global.logger.error('积分流水保存(T_POINT_DETAIL_TMP)--失败--:%s' % e)
                sql = "DELETE FROM T_POINT_DETAIL_TMP WHERE BAK2=?"
                Global.db2_helper.execute(sql, [job_id, ])
                Global.logger.info('积分流水回滚JOB_ID：%s' % job_id)
                raise SQLError('积分流水保存(T_POINT_DETAIL_TMP)--失败--:%s' % e)

    def _save_account_tmp(self):
        """
        将T_POINT_DETAIL_TMP中的所有数据按CUST_NO累加，
        并存入T_POINT_ACCOUNT_TMP
        :return:
        """


        his_tmp_df = self.spark.load_from_db2('T_POINT_DETAIL_TMP').selectExpr('trim(CUST_NO)', 'POINTS_VAL').withColumnRenamed('trim(CUST_NO)','CUST_NO')
        his_sum_df = his_tmp_df.groupBy('CUST_NO').sum('POINTS_VAL').withColumnRenamed('sum(POINTS_VAL)','POINTS_VAL')
        cust_info = self.spark.load_from_db2('T_POINT_CUST_INFO').selectExpr('trim(CUST_NO)', 'CUST_NAME', 'CRE_DT').withColumnRenamed('trim(CUST_NO)', 'CUST_NO')



        validity_val = Global.db2_helper.fetchone("select VALUE from T_POINT_ADD_PARA where KEY='validity'")[0]
        if validity_val != '-1':
            validity = str(datetime.datetime.now().year + int(validity_val)) + '1231'
        else:
            validity = '99999999'

        join = his_sum_df.join(cust_info, 'CUST_NO', 'left_outer')


        join = join.sort('CRE_DT',ascending=False).groupBy('CUST_NO').agg({'CUST_NO': 'first',
                                                                           'CUST_NAME': 'first',
                                                                           'CRE_DT': 'first',
                                                                           'POINTS_VAL': 'first'})


        join_rdd = join.map(lambda x: Row(CUST_NO=x['first(CUST_NO)'],
                                          VALIDITY=validity,
                                          CUST_NAME=x['first(CUST_NAME)'] if x['first(CUST_NAME)'] is not None else '',
                                          POINTS_VAL=x['first(POINTS_VAL)'],
                                          TIMESTAMP=str(time.time()).replace('.','')))

        try:

            # 必须现清空账户暂存表，否则无法插入
            self.clear_point_account_tmp()

            result_df = self.spark.sqlctx.createDataFrame(join_rdd)
            self.spark.write_jdbc(result_df, 'T_POINT_ACCOUNT_TMP', 'append')
            Global.logger.info('积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----成功')

        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有积分流水信息')
                Global.logger.info('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----成功（无数据写入）')
                raise RDDEmptyError('RDD is empty:没有积分流水信息')
            else:
                Global.db2_helper.cnn.rollback()
                Global.logger.error('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----失败---%s' % e)
                raise SQLError('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----失败---%s' % e)

    def _account_tmp_to_real(self):
        """
            merge T_POINT_ACCOUNT_TMP --> T_POINT_ACCOUNT
            insert T_POINT_DETAIL_TMP --> T_POINT_DETAIL
            :return:
        """
        try:
            # 账户暂存表到账户表
            merge_sql = "MERGE INTO T_POINT_ACCOUNT t1 " \
                   "USING T_POINT_ACCOUNT_TMP as t2 " \
                   "ON t1.CUST_NO = t2.CUST_NO AND t1.VALIDITY = t2.VALIDITY" \
                   " WHEN MATCHED THEN " \
                        "UPDATE SET t1.CUST_NAME=t2.CUST_NAME, t1.POINTS_VAL = t1.POINTS_VAL+t2.POINTS_VAL, " \
                        "t1.TIMESTAMP=t2.TIMESTAMP, t1.BAK1=t2.BAK1 ,t1.BAK2=t2.BAK2 " \
                   "WHEN NOT MATCHED THEN " \
                        "INSERT (t1.CUST_NO,t1.VALIDITY, t1.CUST_NAME, t1.POINTS_VAL, t1.TIMESTAMP, t1.BAK1, t1.BAK2) " \
                        "VALUES (t2.CUST_NO,t2.VALIDITY, t2.CUST_NAME, t2.POINTS_VAL, t2.TIMESTAMP, t2.BAK1, t2.BAK2)"


            Global.db2_helper.execute(merge_sql)
            # 更新到汇总表成功后，再将流水数据存到流水表
            Global.logger.info('积分累加到账户表(merge T_POINT_ACCOUNT_TMP --> T_POINT_ACCOUNT) ---- 成功')
        except Exception as e:
            # 清空流水数据暂存表
            Global.db2_helper.cnn.rollback()
            Global.logger.error('积分累加到账户表---- 失败---%s' % e)
            raise SQLError('积分累加到账户表---- 失败---%s' % e)

        try:
            # 流水数据从暂存表 保存到流水表
            sql4 = "insert into T_POINT_DETAIL SELECT * FROM T_POINT_DETAIL_TMP"
            Global.db2_helper.execute(sql4)
            Global.logger.info('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 成功')

            # 清空积分流水暂存表
            self.clear_point_detail_tmp()

        except Exception as e:
            Global.db2_helper.cnn.rollback()
            Global.logger.error('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 失败---%s' % e)
            raise SQLError('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 失败---%s' % e)

        # 黑名单用户积分清零
        self._point_to_zero()


    def _point_to_zero(self):
        """
        积分清零，并入流水表
        :return:
        """
        # 需要清清零的账户
        clear_df = self.init_black_list()
        # 积分账户
        account_df = self.spark.load_from_db2('T_POINT_ACCOUNT').select('CUST_NO','POINTS_VAL')
        # join
        if clear_df is None:
            return

        clear_df = clear_df.join(account_df,'CUST_NO','left_outer').filter("POINTS_VAL!=0")

        # 批量清零
        try:
            sql = "update T_POINT_ACCOUNT set POINTS_VAL=0 WHERE CUST_NO=?"
            Global.db2_helper.batch_operate_commit_final(sql, clear_df.map(lambda x: (x['CUST_NO'],)))
            Global.logger.info('黑名单账户清零--成功（T_POINT_ACCOUNT)')
        except Exception as e:
            Global.db2_helper.cnn.rollback()
            Global.logger.error(e)
            raise BatchSQLError('黑名单账户清零--失败（T_POINT_ACCOUNT)')



        # 生成清零流水数据
        def m(x):
            cust_no = x['CUST_NO']
            point = '-'+str(x['POINTS_VAL'])
            points_type = '1005'

            point_id = str(uuid.uuid1())
            now = datetime.datetime.now()
            occur_date = now.strftime('%Y%m%d')
            occur_time = now.strftime('%H%M%S')
            timestamp = str(time.time()).replace('.', '')
            return Row(SYS_SERIAL_NO=point_id, OCCUR_DATE=occur_date, OCCUR_TIME=occur_time, POINTS_TYPE=points_type,
                       CUST_NO=cust_no, POINTS_VAL=point, STATUS='0', TIMESTAMP=timestamp, REMARK='积分清零',
                       BAK2='积分清零')

        try:
            detail_df = self.spark.sqlctx.createDataFrame(clear_df.map(m))
            self.spark.write_jdbc(detail_df, 'T_POINT_DETAIL', 'append')
            Global.logger.info('积分清零流水保存--成功（T_POINT_DETAIL)')
        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有清零流水')
            else:
                Global.logger.error('积分清零流水保存--失败（T_POINT_DETAIL)')
                Global.logger.error(e)





    # ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ 保存、汇总 积分流水方法 END ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑

    # ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 清空暂存表方法 START ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ ↓ 
    
    def clear_point_detail_tmp(self):
        Global.db2_helper.cnn.commit()
        Global.db2_helper.execute("TRUNCATE TABLE T_POINT_DETAIL_TMP IMMEDIATE")
        Global.logger.info('清空 T_POINT_DETAIL_TMP ---- 成功')

    def clear_point_account_tmp(self):
        Global.db2_helper.cnn.commit()
        Global.db2_helper.execute("TRUNCATE TABLE T_POINT_ACCOUNT_TMP IMMEDIATE")
        Global.logger.info('清空 T_POINT_ACCOUNT_TMP ---- 成功')
    
    # ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ 清空暂存表方法 END ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑


    def init_black_list(self):
        """
        初始化黑名单
        :return:
        """

        black_origin_df = self.spark.load_from_db2('T_POINT_BLACK')

        clear_types = Global.configure.get('param','overdue_owe_clear').split(',')
        noincrease_types = Global.configure.get('param','overdue_owe_noincrease').split(',')

        Global.logger.debug('clear_types:%s' % clear_types)
        Global.logger.debug('noincrease_types:%s' % noincrease_types)

        #black_origin_df.show()


        def m(x):

            if x['BLACK_TYPE'] in clear_types:
                opt = 'clear'
            else:
                opt = 'noincrease'
            return x['CUST_NO'],opt


        def r(x,y):
            if x == 'clear' or y == 'clear':
                return 'clear'
            else:
                return 'noincrease'

        result = black_origin_df.map(m).reduceByKey(r)

        try:
            clear_df = self.spark.sqlctx.createDataFrame(result,['CUST_NO','OPT']).filter("OPT='clear'").select('CUST_NO')
        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有黑名单_清零用户')
            else:
                Global.logger.error('积分清零流水保存--失败（T_POINT_DETAIL)')
                Global.logger.error(e)
            return None

        return clear_df





    def update_relation(self):
        # 更新各类帐号关联
        try:
            # create model saving path
            path = self.spark.hdfs_base + 'relation'

            relation_acc = self.spark.load_from_db2('T_POINT_RELATION_ACC')
            relation_cust = self.spark.load_from_db2('T_POINT_RELATION_CUST')
            relation_join = relation_acc.join(relation_cust, 'CUST_ID', 'outer')

            writer = DataFrameWriter(relation_join)
            writer.save(path, mode='overwrite')
            Global.logger.info('更新帐号关联Dataframe成功!')
            return 'success!'
        except Exception as e:
            Global.logger.error('更新帐号关联Dataframe失败!')
            return 'failure! please run again.'

    def _get_relation(self):
        # 从hdfs上读取关联信息
        path = self.spark.hdfs_base + 'relation'
        reader = DataFrameReader(self.spark.sqlctx)
        try:
            df = reader.load(path)
        except Exception as e:
            self.update_relation()
            df = reader.load(path)
        return df


    # -----------------------------------Below is rowback functions-----------------------------------------#

    #1.根据日期/job_id找到t_point_detail中的明细列表
    #2.先回滚积分清理条目
    #3.再回滚积分加减条目
    #4.删除t_point_detail相应记录


    def rowback_by_jobid(self,job_id,validity):
        self._rowback(job_id)
        self._rowback_save_account_tmp(validity)
        self._rowback_account_tmp_to_real()

    def rowback_by_date(self,data_date,validity):
        sql = "select distinct(job_id) from t_point_spark_job where data_date=? and result='0'"
        result = Global.db2_helper.fetchall(sql,[data_date,])
        Global.logger.info('回滚job_id:')
        Global.logger.info(result)
        self.clear_point_detail_tmp()

        for job_id in result:
            self._rowback(job_id[0])
        self._rowback_save_account_tmp(validity)
        self._rowback_account_tmp_to_real()

    def rowback_by_rule(self,rule_id,data_date_start,data_date_end,validity):

        sql = "select distinct(job_id) from t_point_spark_job where rule_id=? and data_date>=? and data_date<=? and result='0'"
        result = Global.db2_helper.fetchall(sql,[rule_id,data_date_start,data_date_end])
        Global.logger.info('回滚job_id:')
        Global.logger.info(result)
        self.clear_point_detail_tmp()

        for job_id in result:
            self._rowback(job_id[0])
        self._rowback_save_account_tmp(validity)
        self._rowback_account_tmp_to_real()



    def _rowback(self, job_id):

        detail_df = self.spark.load_from_db2('T_POINT_DETAIL').filter("BAK2=%s" % job_id)
        print(detail_df.count())
        black_df = self.spark.load_from_db2('T_POINT_BLACK').select('CUST_NO')

        # 去除黑名单用户（黑名单用户直接归零）
        detail_df = detail_df.subtract(detail_df.join(black_df, 'CUST_NO', 'inner'))
        print(detail_df.count())

        def m(x):
            points_val = -x['POINTS_VAL']
            if points_val >= 0:
                points_type = '0004'
            else:
                points_type = '1005'

            point_id = str(uuid.uuid1())
            now = datetime.datetime.now()
            occur_date = now.strftime('%Y%m%d')
            occur_time = now.strftime('%H%M%S')
            timestamp = str(time.time()).replace('.', '')
            return Row(SYS_SERIAL_NO=point_id, OCCUR_DATE=occur_date, OCCUR_TIME=occur_time, POINTS_TYPE=points_type,
                       CUST_NO=x['CUST_NO'], POINTS_VAL=points_val, STATUS='0', TIMESTAMP=timestamp, REMARK='积分回滚',
                       BAK2=job_id)

        try:
            point_history_df = self.spark.sqlctx.createDataFrame(detail_df.map(m)).filter(
                "CUST_NO!=''")  # 去除客户号不是10开头的数据
            self.spark.write_jdbc(point_history_df, 'T_POINT_DETAIL_TMP', 'append')
            Global.logger.info('积分回滚流水保存(T_POINT_DETAIL_TMP)--成功')

        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有积分回滚流水信息')
            else:
                Global.logger.error('积分回滚流水保存(T_POINT_DETAIL_TMP)--失败--:%s' % e)
                sql = "DELETE FROM T_POINT_DETAIL_TMP WHERE BAK2=?"
                Global.db2_helper.execute(sql, [job_id, ])
                Global.logger.info('积分回滚流水回滚JOB_ID：%s' % job_id)
                raise SQLError('积分回滚流水保存(T_POINT_DETAIL_TMP)--失败--:%s' % e)

    def _rowback_save_account_tmp(self,validity):
        """
        将T_POINT_DETAIL_TMP中的所有数据按CUST_NO累加，
        并存入T_POINT_ACCOUNT_TMP
        :return:
        """


        his_tmp_df = self.spark.load_from_db2('T_POINT_DETAIL_TMP').selectExpr('trim(CUST_NO)', 'POINTS_VAL').withColumnRenamed('trim(CUST_NO)','CUST_NO')
        his_sum_df = his_tmp_df.groupBy('CUST_NO').sum('POINTS_VAL').withColumnRenamed('sum(POINTS_VAL)','POINTS_VAL')
        cust_info = self.spark.load_from_db2('T_POINT_CUST_INFO').selectExpr('trim(CUST_NO)', 'CUST_NAME', 'CRE_DT').withColumnRenamed('trim(CUST_NO)', 'CUST_NO')

        join = his_sum_df.join(cust_info, 'CUST_NO', 'left_outer')


        join = join.sort('CRE_DT',ascending=False).groupBy('CUST_NO').agg({'CUST_NO': 'first',
                                                                           'CUST_NAME': 'first',
                                                                           'CRE_DT': 'first',
                                                                           'POINTS_VAL': 'first'})

        join_rdd = join.map(lambda x: Row(CUST_NO=x['first(CUST_NO)'],
                                          VALIDITY=validity,
                                          CUST_NAME=x['first(CUST_NAME)'] if x['first(CUST_NAME)'] is not None else '',
                                          POINTS_VAL=x['first(POINTS_VAL)'],
                                          TIMESTAMP=str(time.time()).replace('.','')))

        try:

            # 必须现清空账户暂存表，否则无法插入
            self.clear_point_account_tmp()

            result_df = self.spark.sqlctx.createDataFrame(join_rdd)
            self.spark.write_jdbc(result_df, 'T_POINT_ACCOUNT_TMP', 'append')
            Global.logger.info('积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----成功')

        except Exception as e:
            if 'RDD is empty' == str(e):
                Global.logger.info('RDD is empty:没有积分流水信息')
                Global.logger.info('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----成功（无数据写入）')
                raise RDDEmptyError('RDD is empty:没有积分流水信息')
            else:
                Global.db2_helper.cnn.rollback()
                Global.logger.error('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----失败---%s' % e)
                raise SQLError('多个规则积分累计 T_POINT_DETAIL_TMP --> T_POINT_ACCOUNT_TMP-----失败---%s' % e)

    def _rowback_account_tmp_to_real(self):
        """
            merge T_POINT_ACCOUNT_TMP --> T_POINT_ACCOUNT
            insert T_POINT_DETAIL_TMP --> T_POINT_DETAIL
            :return:
        """
        try:
            # 账户暂存表到账户表
            merge_sql = "MERGE INTO T_POINT_ACCOUNT t1 " \
                   "USING T_POINT_ACCOUNT_TMP as t2 " \
                   "ON t1.CUST_NO = t2.CUST_NO AND t1.VALIDITY = t2.VALIDITY" \
                   " WHEN MATCHED THEN " \
                        "UPDATE SET t1.CUST_NAME=t2.CUST_NAME, t1.POINTS_VAL = t1.POINTS_VAL+t2.POINTS_VAL, " \
                        "t1.TIMESTAMP=t2.TIMESTAMP, t1.BAK1=t2.BAK1 ,t1.BAK2=t2.BAK2 " \
                   "WHEN NOT MATCHED THEN " \
                        "INSERT (t1.CUST_NO,t1.VALIDITY, t1.CUST_NAME, t1.POINTS_VAL, t1.TIMESTAMP, t1.BAK1, t1.BAK2) " \
                        "VALUES (t2.CUST_NO,t2.VALIDITY, t2.CUST_NAME, t2.POINTS_VAL, t2.TIMESTAMP, t2.BAK1, t2.BAK2)"


            Global.db2_helper.execute(merge_sql)

            # 积分为负数的改为0
            set_zero_sql = "update t_point_account set points_val=0 where points_val<0"
            Global.db2_helper.execute(set_zero_sql)


            # 更新到汇总表成功后，再将流水数据存到流水表
            Global.logger.info('积分累加到账户表(merge T_POINT_ACCOUNT_TMP --> T_POINT_ACCOUNT) ---- 成功')
        except Exception as e:
            # 清空流水数据暂存表
            Global.db2_helper.cnn.rollback()
            Global.logger.error('积分累加到账户表---- 失败---%s' % e)
            raise SQLError('积分累加到账户表---- 失败---%s' % e)

        try:
            # 流水数据从暂存表 保存到流水表
            sql4 = "insert into T_POINT_DETAIL SELECT * FROM T_POINT_DETAIL_TMP"
            Global.db2_helper.execute(sql4)
            Global.logger.info('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 成功')

            # 清空积分流水暂存表
            self.clear_point_detail_tmp()

        except Exception as e:
            Global.db2_helper.cnn.rollback()
            Global.logger.error('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 失败---%s' % e)
            raise SQLError('流水数据 T_POINT_DETAIL_TMP --> T_POINT_DETAIL ---- 失败---%s' % e)

    # -----------------------------------Above is rowback functions-----------------------------------------#







    def _test(self):
        # selection = ['CUST_NO','DEBT_DAY*DEBT_RBAL']
        # df = self.spark.load_from_db2('T_POINT_CREDIT_CARD').groupBy('CUST_NO').count()
        # df.show()
        sql3 = "MERGE INTO T_POINT_ACCOUNT t1 " \
               "USING T_POINT_ACCOUNT_TMP as t2 " \
               "ON t1.CUST_NO = t2.CUST_NO AND t1.VALIDITY = t2.VALIDITY " \
               "WHEN MATCHED THEN " \
                   "UPDATE SET t1.CUST_NAME=t2.CUST_NAME, t1.POINTS_VAL = t1.POINTS_VAL+t2.POINTS_VAL, " \
                   "t1.TIMESTAMP=t2.TIMESTAMP, t1.BAK1=t2.BAK1 ,t1.BAK2=t2.BAK2 " \
               "WHEN NOT MATCHED THEN " \
                   "INSERT (t1.CUST_NO, t1.VALIDITY, t1.POINTS_VAL, t1.TIMESTAMP, t1.BAK1, t1.BAK2) " \
                   "VALUES (t2.CUST_NO, t2.VALIDITY, t2.POINTS_VAL, t2.TIMESTAMP, t2.BAK1, t2.BAK2)"

        Global.db2_helper.execute(sql3)






if __name__ == '__main__':
    t = PointEngine()
    t._point_to_zero()

    # df = t.spark.load_from_db2('T_POINT_FUND_TRANS')
    # print(df.count())
    #
    #
    # df = df.filter('TRANS_AMT*FUND_TERM/365 >10000')
    # print(df.count())
    # df.show()
    # selection = ['CUST_NO','TRANS_AMT*FUND_TERM/365']
    # c = df.selectExpr(selection).withColumnRenamed(selection[0], 'PK').withColumnRenamed(selection[1], 'VAL')
    # c.show()

