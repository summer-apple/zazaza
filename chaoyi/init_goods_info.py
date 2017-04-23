

# 1.获取商品表信息 存到goods_tmp
# 2.奖商品表去重后，分类组合成1个字段 存入goods
# 3.获取商品子类 存入sub_goods_tmp
# 4.商品表中获取ID信息，left join sub_goods_tmp ,去除重复子商品
# 5.插入商品分类


from chaoyi.mssqlconn import MsSQLHelper
from chaoyi.mysqlconn import MySQLHelper

mssql_helper = MsSQLHelper('122.114.94.176', 'qr_cycs', 'HIuDxxTwb78eyy0O', 'qr_cycs',False)
mysql_helper = MySQLHelper('localhost','root','123456','db_chaoyi',as_dict=False)


TRUNCATE_TABLE_SQL = "truncate table %s"

mysql_helper.execute(TRUNCATE_TABLE_SQL % 'goods')
mysql_helper.execute(TRUNCATE_TABLE_SQL % 'goods_tmp')
mysql_helper.execute(TRUNCATE_TABLE_SQL % 'sub_goods')
mysql_helper.execute(TRUNCATE_TABLE_SQL % 'sub_goods_tmp')
mysql_helper.execute(TRUNCATE_TABLE_SQL % 'category')



# step 1
fetch_goods_sql = "select id goods_id,title goods_no,category_id,img_url from dt_article where id>158"
insert_goods_sql = "insert into goods_tmp(goods_id,goods_no,category_id,img_url) values(%s,%s,%s,%s)"

goods_data = mssql_helper.fetchall(fetch_goods_sql)
mysql_helper.executemany(insert_goods_sql,goods_data)





# step 2
conbin_category_sql = "insert into goods(goods_id,goods_no,category_id,img_url) " \
                      "select  goods_id,goods_no,category_id,img_url " \
                      "FROM goods_tmp GROUP BY goods_no order by goods_id"

mysql_helper.execute(TRUNCATE_TABLE_SQL % 'goods')
mysql_helper.execute(conbin_category_sql)



# step 3

fetch_subgoods_sql = "SELECT t1.id sub_goods_id,t1.article_id goods_id,t2.title goods_no,t1.spec_text " \
                     "model,t1.sell_price price ,t1.img_url " \
                     "FROM dt_article_goods t1 JOIN dt_article t2 on t1.article_id = t2.id"

insert_subgoods_sql = "insert into sub_goods_tmp(sub_goods_id,goods_id,goods_no,model,price,img_url) " \
                      "values(%s,%s,%s,%s,%s,%s)"

subgoods_data = mssql_helper.fetchall(fetch_subgoods_sql)
mysql_helper.executemany(insert_subgoods_sql,subgoods_data)



# step 4

remove_dublicate_and_insert_sql = "insert into sub_goods(sub_goods_id,goods_id,goods_no,model,price,img_url) " \
                       "select t2.sub_goods_id sub_goods_id,t2.goods_id goods_id,t2.goods_no goods_no," \
                       "t2.model model,t2.price price, t2.img_url img_url from goods t1 " \
                       "left join sub_goods_tmp t2 on t1.goods_id=t2.goods_id"


mysql_helper.execute(remove_dublicate_and_insert_sql)


# step 5

fetch_category_sql = "select id category_id,title category_name,parent_id,class_list,class_layer,sort_id from dt_article_category"
insert_category_sql = "insert into category(category_id,category_name,parent_id,class_list,class_layer,sort_id) values(%s,%s,%s,%s,%s,%s)"

category_data = mssql_helper.fetchall(fetch_category_sql)
mysql_helper.executemany(insert_category_sql,category_data)
