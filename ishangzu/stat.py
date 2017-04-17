import os,sys
import fire
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

        # join 埋点详情
        burid_point_df = self.spark.load_from_mysql('page_click_buried_point').select('mark_type', 'event_name')
        join = detail_split_df.join(burid_point_df, 'mark_type', 'left_outer').sort(['daystr','cityname','mark_type'],ascending=[1,1,1])

        # 入库
        self.spark.write_jdbc(join,'page_click_stat','append')

    def test(self):
        button_df = self.spark.load_from_mysql('online007_ctr').filter(
            "classname in ('info_im_m','info_phone_m','zuke_submit_uid','yezhu_submit_uid')")
        button_df.show()

if __name__ == '__main__':

    fire.Fire(Stat)

