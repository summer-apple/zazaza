import os,sys
import fire
import pandas as pd

try:
    from spark_env import SparkEnvironment

except ImportError:
    sys.path.append(os.path.abspath('../'))
    from ishangzu.spark_env import SparkEnvironment




class Stat:
    def __init__(self):
        self.spark = SparkEnvironment(app_name='Stat')


    def truncate_table(self):
        self.spark.mysql_helper.execute("truncate table page_click_stat")

    def clear_stat_day(self, daystr):
        self.spark.mysql_helper.execute("delete from page_click_stat where daystr=%s", [daystr,])

    def stat(self, daystr=None):


        # 按钮埋点数据
        button_df = self.spark.load_from_mysql('online007_ctr').withColumnRenamed('classname','mark_type')\
                                                                .withColumnRenamed('day','daystr')\
                                                                .filter("mark_type in ('info_im_m','info_phone_m','zuke_submit_uid','yezhu_submit_uid')")

        # 链接分析数据
        page_detail_df = self.spark.load_from_mysql('page_click_detail') #.filter("mark_type='bj2-zj1-od1'")


        # 如果要过滤日期按天跑
        if daystr is not None:
            button_df = button_df.filter("daystr=%s" % daystr)
            page_detail_df = page_detail_df.filter("daystr=%s" % daystr)

            self.clear_stat_day(daystr)
        else:
            self.truncate_table()



        def fm(x):
            mark_types = x['mark_type'].split('-')
            return [(x['daystr'],x['cityname'],mt,x['click_count']) for mt in mark_types if mt != '']


        # 先将按钮埋点数据和链接数据union，然后flatmap
        detail_split_rdd = page_detail_df.unionAll(button_df).rdd.flatMap(fm)

        # 转换成df
        detail_split_df = self.spark.sqlctx.createDataFrame(detail_split_rdd, ['daystr','cityname','mark_type','click_count'])

        # group by 然后点击量求和
        detail_split_df = detail_split_df.groupBy(['daystr','cityname','mark_type']).sum('click_count').withColumnRenamed('sum(click_count)','click_count')

        # 埋点详情
        burid_point_df = self.spark.load_from_mysql('page_click_buried_point').select('mark_type', 'event_name','cityname')




        # 点击量为0的埋点数据统计
        clicked_day_df = detail_split_df.select('cityname','mark_type','daystr').distinct()

        day_list = clicked_day_df.select('daystr','cityname').distinct()

        city_mark_day_zip_df = burid_point_df.select('mark_type', 'cityname').join(day_list,'cityname','left_outer')
        """
        +--------+---------+----------+
        |cityname|mark_type|    daystr|
        +--------+---------+----------+
        |      sh|  huangpu|2017-04-14|
        |      sh|  huangpu|2017-04-12|
        |      sh|  huangpu|2017-04-16|
        """


        # z = [(x,y[0],y[1]) for x in day_list for y in burid_point_list]
        # print(z)


        # full_day_mark_cuple = self.spark.sqlctx.createDataFrame(z,['daystr','mark_type'])
        #
        zero_click_df = city_mark_day_zip_df.subtract(clicked_day_df)
        zero_click_df = self.spark.sqlctx.createDataFrame(zero_click_df.rdd.map(lambda x:[x['daystr'],x['cityname'],x['mark_type'],0]),
                                                                                            ['daystr','cityname','mark_type','click_count'])



        # union--join event_name--sort--insert db
        union = detail_split_df.unionAll(zero_click_df)\
                    .join(burid_point_df, ['cityname','mark_type'], 'left_outer')\
                    .sort(['daystr', 'cityname', 'mark_type'], ascending=[1, 1, 1])


        self.spark.write_jdbc(union, 'page_click_stat', 'append')


    def stat_by_city(self,cityname,daystr):
        stat_df = self.spark.load_from_mysql('page_click_stat').filter("cityname='%s'" % cityname).filter("daystr='%s'" % daystr)
        buried_df = self.spark.load_from_mysql('local_buried').filter("cityname='%s'" % cityname).select('idx','event_name')

        join = buried_df.join(stat_df,'event_name','left_outer')

        def m(x):

            mark_type = x['mark_type'] if x['mark_type'] is not None else ''
            click_count = x['click_count'] if x['click_count'] is not None else 0

            return x['idx'],daystr,cityname,mark_type,x['event_name'],click_count

        rdd = join.rdd.map(m)

        df = self.spark.sqlctx.createDataFrame(rdd,['idx','daystr','cityname','mark_type','event_name','click_count'])
        self.spark.write_jdbc(df,'page_click_stat_report','append')


    def test(self):
        button_df = self.spark.load_from_mysql('online007_ctr').filter(
            "classname in ('info_im_m','info_phone_m','zuke_submit_uid','yezhu_submit_uid')")
        button_df.show()

if __name__ == '__main__':

    #fire.Fire(Stat)
    s = Stat()
    s.stat()


