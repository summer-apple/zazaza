# -*- coding: utf-8 -*-

import ibm_db_dbi

try:
    from code_generator import CodeGenerator
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath('../'))
    from product.code_generator import CodeGenerator


class DB2Helper:
    def __init__(self,host,port,database,user,password,protocol):
        self.dsn = "driver={IBM DB2 ODBC DRIVER};database=%s;hostname=%s;port=%s;protocol=%s;" % (
    database, host, port,protocol)

        try:
            self.cnn = ibm_db_dbi.connect(self.dsn, user=user, password=password)
            self.cur = self.cnn.cursor()
        except Exception as e:
            print('Connect to DB2 fails! {}'.format(e))

    def _strip(self, obj):
        '''
        将db2取回的数据trim
        :param obj:
        :return:
        '''

        def x_strip(tp):
            tmp = []
            for t in tp:
                if type(t) == str:
                    tmp.append(t.strip())
                else:
                    tmp.append(t)
            return tmp
        if obj is None:
            return None
        elif type(obj) == list:
            list_tmp = []
            for o in obj:
                list_tmp.append(x_strip(o))
            return list_tmp
        else:
            return x_strip(obj)

    def execute(self, sql, args=()):
        self.cur.execute(sql, args)
        self.cnn.commit()

    def execute_without_commit(self, sql, args=()):
        self.cur.execute(sql, args)

    def executemany(self, sql, args=()):
        self.cur.executemany(sql, args)
        self.cnn.commit()

    def executemany_without_commit(self, sql, args=()):
        self.cur.executemany(sql, args)

    def fetchone(self, sql, args=()):
        self.cur.execute(sql, args)
        return self._strip(self.cur.fetchone())

    def fetchmany(self, sql, size, args=()):
        self.cur.execute(sql, args)
        return self._strip(self.cur.fetchmany(size))

    def fetchall(self, sql, args=()):
        self.cur.execute(sql, args)
        return self._strip(self.cur.fetchall())

    def batch_operate(self, sql, rdd, once_size=1000):
        '''
        批量数据库操作
        :param sql:要批量执行的语句
        :param rdd:数据源RDD，经过Map操作得到的tuple列表[(a,b,c),(d,e,f),(d.f.g)]
        :param once_size:每次执行的条数，默认每次一千条
        :return:
        '''
        temp = []
        for row in rdd.collect():
            if len(temp) >= once_size:
                self.executemany(sql, temp)
                temp.clear()
            temp.append(row)

        if len(temp) != 0:
            self.executemany(sql, temp)
            temp.clear()

    def batch_operate_commit_final(self, sql, rdd, once_size=1000):
        '''
        批量数据库操作
        :param sql:要批量执行的语句
        :param rdd:数据源RDD，经过Map操作得到的tuple列表[(a,b,c),(d,e,f),(d.f.g)]
        :param once_size:每次执行的条数，默认每次一千条
        :return:
        '''
        temp = []
        for row in rdd.collect():
            if len(temp) >= once_size:
                self.executemany_without_commit(sql, temp)
                temp.clear()
            temp.append(row)

        if len(temp) != 0:
            self.executemany_without_commit(sql, temp)
            temp.clear()

        # commit finally
        self.cnn.commit()


    def save_point_to_tmp(self, sql, rdd, once_size=1000):
        '''
        批量数据库操作
        :param sql:要批量执行的语句
        :param rdd:数据源RDD，经过Map操作得到的tuple列表[(a,b,c),(d,e,f),(d.f.g)]
        :param once_size:每次执行的条数，默认每次一千条
        :return:
        '''
        cg = CodeGenerator()
        current_args = list()
        args = rdd.collect()


        for i in range(len(args)):
            if len(args) != 0:
                full_arg = [cg.get_sequence('point'),]
                full_arg.extend(args.pop())
                current_args.append(full_arg)

            # 但前参数长度达到批量值 或者 剩余参数已为0
            if len(current_args) >= once_size or len(args) == 0:
                if len(current_args) > 0:
                    try:
                        self.executemany_without_commit(sql, current_args)
                    except Exception as e:
                        return e, current_args
                    current_args.clear()
                    self.cnn.commit()


    def close(self):
        self.cur.close()
        self.cnn.close()

    def test_commit(self):
        import datetime
        a = 0


        sql = "insert into T_POINT_TEST(T_ID, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        t1 = datetime.datetime.now()
        for i in range(1000):

            p = []
            for i in range(1000):
                p.append(
                    (a, 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa',
                     'aaaaaaaaaaaaaaaaaaaa'
                     , 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaa',
                     'aaaaaaaaaaaaaaaaaaaa'))
                a = a + 1

            t1 = datetime.datetime.now()
            self.execute_without_commit(sql, p)
            t2 = datetime.datetime.now()
            print((t2-t1).seconds)






if __name__ == '__main__':
    driver = 'com.ibm.db2.jcc.DB2Driver'
    host = '192.168.17.40'
    port = '60012'
    database = 'pob2'
    user = 'pob2'
    password = 'pob2'
    protocol = 'tcpip'

    helper = DB2Helper(host,port,database,user,password,protocol)
    result = helper.fetchall("SELECT DISTINCT(RULE_ID),SOURCE_TABLE FROM T_POINT_PROP")

    for r in result:
        print(r)
        helper.execute("update T_POINT_RULE set SOURCE_TABLE='%s' where RULE_ID='%s'" % (r[1], r[0]))