import pymssql

# server    数据库服务器名称或IP
# user      用户名
# password  密码
# database  数据库名称


class MsSQLHelper():
    def __init__(self,host,user,passowrd,db,as_dict=True):
        self.conn = pymssql.connect(host,user,passowrd,db)
        self.cursor = self.conn.cursor(as_dict = as_dict)



    def execute(self,sql,args=[]):
        self.cursor.execute(sql,args)
        self.conn.commit()

    def executemany(self,sql,args=[]):
        self.cursor.execute(sql,args)
        self.conn.commit()

    def fetchone(self, sql, args=[]):
        self.cursor.execute(sql, args)
        return  self.cursor.fetchone()

    def fetchall(self, sql, args=[]):
        self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':

    ms_helper = MsSQLHelper('122.114.94.176', 'qr_cycs', 'HIuDxxTwb78eyy0O', 'qr_cycs',False)
    result = ms_helper.fetchall("select * from dt_article_goods")
    for r in result:
        print(float(r[7]))