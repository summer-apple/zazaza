import pymysql


class MySQLHelper():
    def __init__(self,host,user,password,database='',port=3306,charset='utf8',as_dict=True):
        self.config = {'host': host,
                  'user': user,
                  'password': password,
                  'port': port,
                  'charset':charset,
                  'database':database
                  }

        self.cnn = pymysql.connect(**self.config)
        if as_dict:
            self.cur = self.cnn.cursor(pymysql.cursors.DictCursor)
        else:
            self.cur = self.cnn.cursor()

    def execute(self, sql, args=[]):
        self.cur.execute(sql, args)
        self.cnn.commit()

    def executemany(self, sql, args=[]):
        self.cur.executemany(sql, args)
        self.cnn.commit()

    def fetchone(self, sql, args=[]):
        self.cur.execute(sql, args)
        return self.cur.fetchone()

    def fetchmany(self, sql, args=[], size=None):
        self.cur.execute(sql, args)
        return self.cur.fetchmany(size)

    def fetchall(self, sql, args=[]):
        self.cur.execute(sql, args)
        return self.cur.fetchall()

        # def batch_operate(self, sql, rdd, once_size=1000):
        #     '''
        #     批量数据库操作
        #     :param sql:要批量执行的语句
        #     :param rdd:数据源RDD，经过Map操作得到的tuple列表[(a,b,c),(d,e,f),(d.f.g)]
        #     :param once_size:每次执行的条数，默认每次一千条
        #     :return:
        #     '''
        #     temp = []
        #     for row in rdd.collect():
        #         if len(temp) >= once_size:
        #             self.executemany(sql, temp)
        #             temp.clear()
        #         temp.append(row)
        #
        #     if len(temp) != 0:
        #         self.executemany(sql, temp)
        #         temp.clear()

    def close(self):
        self.cur.close()
        self.cnn.close()

if __name__ == '__main__':

    data = """1	获取点击数量	URL	列表页_租金_1500元以下	zj1
2	获取点击数量	URL	列表页_租金_1500-2000元	zj2
3	获取点击数量	URL	列表页_租金_2000-2500元	zj3
4	获取点击数量	URL	列表页_租金_2500-4000元	zj4
5	获取点击数量	URL	列表页_租金_4000元以上	zj5
6	获取点击数量	URL	列表页_默认排序_租金最低	od1
7	获取点击数量	URL	列表页_默认排序_租金最高	od2
8	获取点击数量	URL	列表页_默认排序_面积最小	od3
9	获取点击数量	URL	列表页_默认排序_面积最大	od4
10	获取点击数量	URL	列表页_范围_1公里以内	bj1
11	获取点击数量	URL	列表页_范围_3公里以内	bj2
12	获取点击数量	URL	列表页_范围_5公里以内	bj3
13	获取点击数量	URL	列表页_范围_7公里以内	bj4
14	获取点击数量	URL	列表页_范围_10公里以内	bj5
15	获取点击数量	URL	列表页_更多_整租	fs1
16	获取点击数量	URL	列表页_更多_合租	fs2
17	获取点击数量	URL	列表页_更多_品牌公寓	lx1
18	获取点击数量	URL	列表页_更多_服务公寓	lx2
19	获取点击数量	URL	列表页_更多_一室	hx1
20	获取点击数量	URL	列表页_更多_二室	hx2
21	获取点击数量	URL	列表页_更多_三室	hx3
22	获取点击数量	URL	列表页_更多_四室	hx4
23	获取点击数量	URL	列表页_更多_五室及以上	hx5
24	获取点击数量	URL	列表页_更多_独卫	ts5
25	获取点击数量	URL	列表页_更多_带阳台	ts1
26	获取点击数量	URL	列表页_更多_朝南	cx3
27	获取点击数量	URL	列表页_区域_上城区_全上城区	shangcheng
28	获取点击数量	URL	列表页_区域_上城区_四季青	sijiqing
29	获取点击数量	URL	列表页_区域_上城区_复兴	fuxing
30	获取点击数量	URL	列表页_区域_上城区_南星桥	nanxingqiao
31	获取点击数量	URL	列表页_区域_上城区_近江	jinjiang
32	获取点击数量	URL	列表页_区域_上城区_望江门	wangjiangmen
33	获取点击数量	URL	列表页_区域_上城区_清泰（城站）	qingtai
34	获取点击数量	URL	列表页_区域_上城区_吴山	wushan
35	获取点击数量	URL	列表页_区域_上城区_菜市桥	caishiqiao
36	获取点击数量	URL	列表页_区域_上城区_解百	jiebai
37	获取点击数量	URL	列表页_区域_上城区_马市街	mashijie
38	获取点击数量	URL	列表页_区域_上城区_河坊街	hefangjie
39	获取点击数量	URL	列表页_区域_全下城区_全下城区	xiacheng
40	获取点击数量	URL	列表页_区域_下城区_潮鸣	chaoming
41	获取点击数量	URL	列表页_区域_下城区_朝晖	zhaohui
42	获取点击数量	URL	列表页_区域_下城区_天水	tianshui
43	获取点击数量	URL	列表页_区域_下城区_孩儿巷	haierxiang
44	获取点击数量	URL	列表页_区域_下城区_和平	heping
45	获取点击数量	URL	列表页_区域_下城区_宝善	baoshan
46	获取点击数量	URL	列表页_区域_下城区_庆春	qingchun
47	获取点击数量	URL	列表页_区域_下城区_西湖文化广场	xihuwenhuaguangchang
48	获取点击数量	URL	列表页_区域_下城区_武林	wulin
49	获取点击数量	URL	列表页_区域_下城区_石桥	shiqiao
50	获取点击数量	URL	列表页_区域_下城区_湖滨	hubin
51	获取点击数量	URL	列表页_区域_下城区_东新	dongxin
52	获取点击数量	URL	列表页_区域_下城区_流水苑	liushuiyuan
53	获取点击数量	URL	列表页_区域_下城区_三塘	santang
54	获取点击数量	URL	列表页_区域_下城区_莫衙营	moyaying
55	获取点击数量	URL	列表页_区域_下城区_长庆	changqing
56	获取点击数量	URL	列表页_区域_全江干区_全江干区	jianggan
57	获取点击数量	URL	列表页_区域_江干区_闸弄口	zhanongkou
58	获取点击数量	URL	列表页_区域_江干区_下沙沿江	xiashayanjiang
59	获取点击数量	URL	列表页_区域_江干区_下沙物美	xiashawumei
60	获取点击数量	URL	列表页_区域_江干区_下沙金沙湖	xiashajinshahu
61	获取点击数量	URL	列表页_区域_江干区_下沙江滨	xiashajiangbin
62	获取点击数量	URL	列表页_区域_江干区_四季青	sijiqing
63	获取点击数量	URL	列表页_区域_江干区_三里亭	sanliting
64	获取点击数量	URL	列表页_区域_江干区_钱江新城	qiangjiangxincheng
65	获取点击数量	URL	列表页_区域_江干区_彭埠	pengbu
66	获取点击数量	URL	列表页_区域_江干区_南肖埠	nanxiaobu
67	获取点击数量	URL	列表页_区域_江干区_九堡	jiubao
68	获取点击数量	URL	列表页_区域_江干区_景芳	jingfang
69	获取点击数量	URL	列表页_区域_江干区_火车东站	huochedongzhan
70	获取点击数量	URL	列表页_区域_江干区_采荷	caihe
71	获取点击数量	URL	列表页_区域_全拱墅区_全拱墅区	gongshu
72	获取点击数量	URL	列表页_区域_拱墅区_桥西	qiaoxi
73	获取点击数量	URL	列表页_区域_拱墅区_米市巷	mishixiang
74	获取点击数量	URL	列表页_区域_拱墅区_大关	daguan
75	获取点击数量	URL	列表页_区域_拱墅区_拱宸桥	gongchenqiao
76	获取点击数量	URL	列表页_区域_拱墅区_卖鱼桥	maiyuqiao
77	获取点击数量	URL	列表页_区域_拱墅区_万达	wanda
78	获取点击数量	URL	列表页_区域_拱墅区_瓜山	guashan
79	获取点击数量	URL	列表页_区域_拱墅区_祥符	xiangfu
80	获取点击数量	URL	列表页_区域_拱墅区_长乐	changle
81	获取点击数量	URL	列表页_区域_全西湖区_全西湖区	xihu
82	获取点击数量	URL	列表页_区域_西湖区_翠苑	cuiyuan
83	获取点击数量	URL	列表页_区域_西湖区_申花	shenhua
84	获取点击数量	URL	列表页_区域_西湖区_文新	wenxin
85	获取点击数量	URL	列表页_区域_西湖区_文苑	wenyuan
86	获取点击数量	URL	列表页_区域_西湖区_益乐	yile
87	获取点击数量	URL	列表页_区域_西湖区_古荡	gudang
88	获取点击数量	URL	列表页_区域_西湖区_学军	xuejun
89	获取点击数量	URL	列表页_区域_西湖区_九莲	jiulian
90	获取点击数量	URL	列表页_区域_西湖区_文教	wenjiao
91	获取点击数量	URL	列表页_区域_西湖区_保俶路	baochulu
92	获取点击数量	URL	列表页_区域_西湖区_黄龙	huanglong
93	获取点击数量	URL	列表页_区域_西湖区_西溪	xixi
94	获取点击数量	URL	列表页_区域_西湖区_转塘	zhuantang
95	获取点击数量	URL	列表页_区域_西湖区_三墩	sandun
96	获取点击数量	URL	列表页_区域_西湖区_骆家庄	luojiazhuang
97	获取点击数量	URL	列表页_区域_全滨江区_全滨江区	binjiang
98	获取点击数量	URL	列表页_区域_滨江区_区政府	quzhengfu
99	获取点击数量	URL	列表页_区域_滨江区_西兴	xixing
100	获取点击数量	URL	列表页_区域_滨江区_长河	changhe
101	获取点击数量	URL	列表页_区域_滨江区_奥体	aoti
102	获取点击数量	URL	列表页_区域_滨江区_浦沿	puyan
103	获取点击数量	URL	列表页_区域_滨江区_滨盛	binsheng
104	获取点击数量	URL	列表页_区域_滨江区_一桥南	yiqiaonan
105	获取点击数量	URL	列表页_区域_滨江区_四桥南	siqiaonan
106	获取点击数量	URL	列表页_区域_全萧山区_全萧山区	xiaoshan
107	获取点击数量	URL	列表页_区域_萧山区_北干	beigan
108	获取点击数量	URL	列表页_区域_萧山区_开发区	kaifaqu
109	获取点击数量	URL	列表页_区域_萧山区_城厢	chengxiang
110	获取点击数量	URL	列表页_区域_萧山区_宁围	ningwei
111	获取点击数量	URL	列表页_区域_萧山区_蜀山	shushan
112	获取点击数量	URL	列表页_区域_全余杭区_全余杭区	yuhang
113	获取点击数量	URL	列表页_区域_余杭区_闲林	xianlin
114	获取点击数量	URL	列表页_区域_余杭区_未来科技城	weilaikejicheng
115	获取点击数量	URL	列表页_区域_余杭区_老余杭	laoyuhang
116	获取点击数量	URL	列表页_区域_余杭区_临平	linping
117	获取点击数量	URL	列表页_区域_余杭区_良渚	liangzhu
118	获取点击数量	URL	列表页_区域_黄浦区_全黄浦区	huangpu
119	获取点击数量	URL	列表页_区域_黄浦区_董家渡	dongjiadu
120	获取点击数量	URL	列表页_区域_黄浦区_蓬莱公园	penglaigongyuan
121	获取点击数量	URL	列表页_区域_黄浦区_打浦桥	dapuqiao
122	获取点击数量	URL	列表页_区域_黄浦区_淮海中路	huaihaizhonglu
123	获取点击数量	URL	列表页_区域_黄浦区_老西门	laoximen
124	获取点击数量	URL	列表页_区域_黄浦区_新天地	xintiandi
125	获取点击数量	URL	列表页_区域_黄浦区_南京东路	nanjingdonglu
126	获取点击数量	URL	列表页_区域_黄浦区_豫园	yuyuan
127	获取点击数量	URL	列表页_区域_黄浦区_五里桥	wuliqiao
128	获取点击数量	URL	列表页_区域_黄浦区_人民广场	renminguangchang
129	获取点击数量	URL	列表页_区域_黄浦区_复兴公园	fuxinggongyuan
130	获取点击数量	URL	列表页_区域_黄浦区_黄浦滨江	huangpubinjiang
131	获取点击数量	URL	列表页_区域_黄浦区_世博滨江	shibobinjiang
132	获取点击数量	URL	列表页_区域_黄浦区_外滩	waitan
133	获取点击数量	URL	列表页_区域_黄浦区_昆山	kunshan
134	获取点击数量	URL	列表页_区域_黄浦区_陆家浜路	lujiabanglu
135	获取点击数量	URL	列表页_区域_徐汇区_全徐汇区	xuhui
136	获取点击数量	URL	列表页_区域_徐汇区_徐家汇	xujiahui
137	获取点击数量	URL	列表页_区域_徐汇区_华东理工	huadongligong
138	获取点击数量	URL	列表页_区域_徐汇区_龙华	longhua
139	获取点击数量	URL	列表页_区域_徐汇区_康健	kangjian
140	获取点击数量	URL	列表页_区域_徐汇区_华泾	huajing
141	获取点击数量	URL	列表页_区域_徐汇区_长桥	changqiao
142	获取点击数量	URL	列表页_区域_徐汇区_漕河泾	caohejing
143	获取点击数量	URL	列表页_区域_徐汇区_淮海西路	huaihaixilu
144	获取点击数量	URL	列表页_区域_徐汇区_衡山路	hengshanlu
145	获取点击数量	URL	列表页_区域_徐汇区_田林	tianlin
146	获取点击数量	URL	列表页_区域_徐汇区_上海南站	shanghainanzhan
147	获取点击数量	URL	列表页_区域_徐汇区_斜土林	xietulin
148	获取点击数量	URL	列表页_区域_徐汇区_万体馆	wantiguan
149	获取点击数量	URL	列表页_区域_徐汇区_建国西路	jianguoxilu
150	获取点击数量	URL	列表页_区域_徐汇区_徐汇滨江	xuhuibinjiang
151	获取点击数量	URL	列表页_区域_徐汇区_复兴中路	fuxingzhonglu
152	获取点击数量	URL	列表页_区域_徐汇区_植物园	zhiwuyuan
153	获取点击数量	URL	列表页_区域_徐汇区_湖南路	hunanlu
154	获取点击数量	URL	列表页_区域_徐汇区_肇嘉浜路	zhaojiabanglu
155	获取点击数量	URL	列表页_区域_徐汇区_龙华	longhua
156	获取点击数量	URL	列表页_区域_徐汇区_滨江	binjiang
157	获取点击数量	URL	列表页_区域_长宁区_全长宁区	changning
158	获取点击数量	URL	列表页_区域_长宁区_虹桥	hongqiao
159	获取点击数量	URL	列表页_区域_长宁区_古北	gubei
160	获取点击数量	URL	列表页_区域_长宁区_西郊	xijiao
161	获取点击数量	URL	列表页_区域_长宁区_北新泾	beixinjing
162	获取点击数量	URL	列表页_区域_长宁区_天山	tianshan
163	获取点击数量	URL	列表页_区域_长宁区_中山公园	zhongshangongyuan
164	获取点击数量	URL	列表页_区域_长宁区_新华路	xinhualu
165	获取点击数量	URL	列表页_区域_长宁区_程家桥	chengjiaqiao
166	获取点击数量	URL	列表页_区域_长宁区_镇宁路	zhenninglu
167	获取点击数量	URL	列表页_区域_长宁区_仙霞	xiaqi
168	获取点击数量	URL	列表页_区域_长宁区_动物园	dongwuyuan
169	获取点击数量	URL	列表页_区域_新静安区_全新静安区	jingan
170	获取点击数量	URL	列表页_区域_新静安区_彭浦	pengpu
171	获取点击数量	URL	列表页_区域_新静安区_不夜城	buyecheng
172	获取点击数量	URL	列表页_区域_新静安区_江宁路	jiangninglu
173	获取点击数量	URL	列表页_区域_新静安区_曹家渡	caojiadu
174	获取点击数量	URL	列表页_区域_新静安区_南京西路	nanjingxilu
175	获取点击数量	URL	列表页_区域_新静安区_大宁	daning
176	获取点击数量	URL	列表页_区域_新静安区_静安寺	jingansi
177	获取点击数量	URL	列表页_区域_新静安区_西藏北路	xizangbeilu
178	获取点击数量	URL	列表页_区域_新静安区_闸北公园	zhabeigongyuan
179	获取点击数量	URL	列表页_区域_新静安区_阳城	yangcheng
180	获取点击数量	URL	列表页_区域_新静安区_永和	yonghe
181	获取点击数量	URL	列表页_区域_新静安区_重固	chonggu
182	获取点击数量	URL	列表页_区域_新静安区_新客站	xinkezhan
183	获取点击数量	URL	列表页_区域_新静安区_汶水路	wenshuilu
184	获取点击数量	URL	列表页_区域_新静安区_七浦路	qipulu
185	获取点击数量	URL	列表页_区域_新静安区_芷江西路	zhijiangxilu
186	获取点击数量	URL	列表页_区域_普陀区_全普陀区	putuo
187	获取点击数量	URL	列表页_区域_普陀区_武宁	wuning
188	获取点击数量	URL	列表页_区域_普陀区_光新	guangxin
189	获取点击数量	URL	列表页_区域_普陀区_桃浦	taopu
190	获取点击数量	URL	列表页_区域_普陀区_真如	zhenru
191	获取点击数量	URL	列表页_区域_普陀区_长寿路	changshouqiao
192	获取点击数量	URL	列表页_区域_普陀区_曹杨	caoyang
193	获取点击数量	URL	列表页_区域_普陀区_长征	changzheng
194	获取点击数量	URL	列表页_区域_普陀区_长风	changfeng
195	获取点击数量	URL	列表页_区域_普陀区_真光	zhenguang
196	获取点击数量	URL	列表页_区域_普陀区_甘泉宜川	ganquanyichuan
197	获取点击数量	URL	列表页_区域_普陀区_万里	wanli
198	获取点击数量	URL	列表页_区域_普陀区_中远两湾城	zhongyuanliangwancheng
199	获取点击数量	URL	列表页_区域_普陀区_长风生态商务区	changfengshengtaishangwuqu
200	获取点击数量	URL	列表页_区域_虹口区_全虹口区	hongkou
201	获取点击数量	URL	列表页_区域_虹口区_临平路	linpinglu
202	获取点击数量	URL	列表页_区域_虹口区_曲阳	quyang
203	获取点击数量	URL	列表页_区域_虹口区_四川北路	sichuanbei
204	获取点击数量	URL	列表页_区域_虹口区_凉城	liangcheng
205	获取点击数量	URL	列表页_区域_虹口区_江湾镇	jiangwanzhen
206	获取点击数量	URL	列表页_区域_虹口区_和平公园	hepinggongyuan
207	获取点击数量	URL	列表页_区域_虹口区_鲁迅公园	luxungongyuan
208	获取点击数量	URL	列表页_区域_虹口区_北外滩	beiwaitan
209	获取点击数量	URL	列表页_区域_虹口区_虹口公园	hongkougongyuan
210	获取点击数量	URL	列表页_区域_杨浦区_全杨浦区	yangpu
211	获取点击数量	URL	列表页_区域_杨浦区_黄兴公园	huangxinggongyuan
212	获取点击数量	URL	列表页_区域_杨浦区_东外滩	dongwaitan
213	获取点击数量	URL	列表页_区域_杨浦区_鞍山	anshan
214	获取点击数量	URL	列表页_区域_杨浦区_五角场	wujiaochang
215	获取点击数量	URL	列表页_区域_杨浦区_中原	zhongyuan
216	获取点击数量	URL	列表页_区域_杨浦区_控江路	kongjianglu
217	获取点击数量	URL	列表页_区域_杨浦区_周家嘴路	zhoujiazuilu
218	获取点击数量	URL	列表页_区域_杨浦区_杨浦大桥	yangpudaqiao
219	获取点击数量	URL	列表页_区域_杨浦区_杨浦公园	yangpugongyuan
220	获取点击数量	URL	列表页_区域_杨浦区_新江湾城	xinjiangwancheng
221	获取点击数量	URL	列表页_区域_闵行区_全闵行区	minhang
222	获取点击数量	URL	列表页_区域_闵行区_颛桥	zhuanqiao
223	获取点击数量	URL	列表页_区域_闵行区_金虹桥	jinhongqiao
224	获取点击数量	URL	列表页_区域_闵行区_七宝	qibao
225	获取点击数量	URL	列表页_区域_闵行区_莘庄	xinzhuang
226	获取点击数量	URL	列表页_区域_闵行区_华漕	huacao
227	获取点击数量	URL	列表页_区域_闵行区_春申	chunshen
228	获取点击数量	URL	列表页_区域_闵行区_浦江	pujiang
229	获取点击数量	URL	列表页_区域_闵行区_老闵行	laominhang
230	获取点击数量	URL	列表页_区域_闵行区_古美	gumei
231	获取点击数量	URL	列表页_区域_闵行区_静安新城	jinganxincheng
232	获取点击数量	URL	列表页_区域_闵行区_马桥	maqiao
233	获取点击数量	URL	列表页_区域_闵行区_江川路	jiangchuanlu
234	获取点击数量	URL	列表页_区域_闵行区_龙柏	longbai
235	获取点击数量	URL	列表页_区域_闵行区_航华	hanghua
236	获取点击数量	URL	列表页_区域_闵行区_金汇	jinhui
237	获取点击数量	URL	列表页_区域_闵行区_梅陇	meilong
238	获取点击数量	URL	列表页_区域_闵行区_吴泾	wujing
239	获取点击数量	URL	列表页_区域_闵行区_罗阳	luoyang
240	获取点击数量	URL	列表页_区域_闵行区_南方商城	nanfangshangcheng
241	获取点击数量	URL	列表页_区域_宝山区_全宝山区	baoshan
242	获取点击数量	URL	列表页_区域_宝山区_淞宝	songbao
243	获取点击数量	URL	列表页_区域_宝山区_杨行	yanghang
244	获取点击数量	URL	列表页_区域_宝山区_大华	dahua
245	获取点击数量	URL	列表页_区域_宝山区_顾村	gucun
246	获取点击数量	URL	列表页_区域_宝山区_通河	tonghe
247	获取点击数量	URL	列表页_区域_宝山区_上大	shangda
248	获取点击数量	URL	列表页_区域_宝山区_张庙	zhangmiao
249	获取点击数量	URL	列表页_区域_宝山区_淞南	songnan
250	获取点击数量	URL	列表页_区域_宝山区_共富	gongfu
251	获取点击数量	URL	列表页_区域_宝山区_共康	gongkang
252	获取点击数量	URL	列表页_区域_宝山区_月浦	yuepu
253	获取点击数量	URL	列表页_区域_宝山区_罗店	luodian
254	获取点击数量	URL	列表页_区域_宝山区_高境	gaojing
255	获取点击数量	URL	列表页_区域_宝山区_大场镇	dachangzhen
256	获取点击数量	URL	列表页_区域_宝山区_泗塘	sitang
257	获取点击数量	URL	列表页_区域_宝山区_罗径	luojing
258	获取点击数量	URL	列表页_区域_嘉定区_全嘉定区	jiading
259	获取点击数量	URL	列表页_区域_嘉定区_菊园新区	juyuanxinqu
260	获取点击数量	URL	列表页_区域_嘉定区_嘉定新城	jiadingxincheng
261	获取点击数量	URL	列表页_区域_嘉定区_嘉定城区	jiadingchengqu
262	获取点击数量	URL	列表页_区域_嘉定区_江桥	jiangqiao
263	获取点击数量	URL	列表页_区域_嘉定区_丰庄	fengzhuang
264	获取点击数量	URL	列表页_区域_嘉定区_真新	zhenxin
265	获取点击数量	URL	列表页_区域_嘉定区_安亭	anting
266	获取点击数量	URL	列表页_区域_嘉定区_南翔	nanxiang
267	获取点击数量	URL	列表页_区域_嘉定区_马陆	malu
268	获取点击数量	URL	列表页_区域_嘉定区_江桥新城	jiangqiaoxincheng
269	获取点击数量	URL	列表页_区域_嘉定区_徐行	xuxing
270	获取点击数量	URL	列表页_区域_浦东新区_全浦东新区	pudongxin
271	获取点击数量	URL	列表页_区域_浦东新区_源深	yuanshen
272	获取点击数量	URL	列表页_区域_浦东新区_张江	zhangjiang
273	获取点击数量	URL	列表页_区域_浦东新区_惠南	huinan
274	获取点击数量	URL	列表页_区域_浦东新区_北蔡	beicai
275	获取点击数量	URL	列表页_区域_浦东新区_川沙	chuansha
276	获取点击数量	URL	列表页_区域_浦东新区_碧云	biyun
277	获取点击数量	URL	列表页_区域_浦东新区_金桥	jinqiao
278	获取点击数量	URL	列表页_区域_浦东新区_陆家嘴	lujiazui
279	获取点击数量	URL	列表页_区域_浦东新区_外高桥	waigaoqiao
280	获取点击数量	URL	列表页_区域_浦东新区_上南	shangnan
281	获取点击数量	URL	列表页_区域_浦东新区_世博	shibo
282	获取点击数量	URL	列表页_区域_浦东新区_南码头	nanmatou
283	获取点击数量	URL	列表页_区域_浦东新区_世纪公园	shijigongyuan
284	获取点击数量	URL	列表页_区域_浦东新区_三林	sanlin
285	获取点击数量	URL	列表页_区域_浦东新区_杨东	yangdong
286	获取点击数量	URL	列表页_区域_浦东新区_洋泾	yangjing
287	获取点击数量	URL	列表页_区域_浦东新区_高行	ganghang
288	获取点击数量	URL	列表页_区域_浦东新区_塘桥	tangqiao
289	获取点击数量	URL	列表页_区域_浦东新区_康桥	kangqiao
290	获取点击数量	URL	列表页_区域_浦东新区_新场	xinchang
291	获取点击数量	URL	列表页_区域_浦东新区_金杨	jinyang
292	获取点击数量	URL	列表页_区域_浦东新区_航头	hangtou
293	获取点击数量	URL	列表页_区域_浦东新区_花木	huamu
294	获取点击数量	URL	列表页_区域_浦东新区_周浦	zhoupu
295	获取点击数量	URL	列表页_区域_浦东新区_唐镇	tangzhen
296	获取点击数量	URL	列表页_区域_浦东新区_曹路	caolu
297	获取点击数量	URL	列表页_区域_浦东新区_联洋	lianyang
298	获取点击数量	URL	列表页_区域_浦东新区_临港新城	lingangxincheng
299	获取点击数量	URL	列表页_区域_浦东新区_梅园	meiyuan
300	获取点击数量	URL	列表页_区域_浦东新区_潍坊	weifang
301	获取点击数量	URL	列表页_区域_浦东新区_八佰伴	babaiban
302	获取点击数量	URL	列表页_区域_浦东新区_祝桥	zhuqiao
303	获取点击数量	URL	列表页_区域_浦东新区_书院镇	shuyuanzhen
304	获取点击数量	URL	列表页_区域_浦东新区_御桥	yuqiao
305	获取点击数量	URL	列表页_区域_金山区_全金山区	jinshan
306	获取点击数量	URL	列表页_区域_金山区_枫泾	fengjing
307	获取点击数量	URL	列表页_区域_金山区_金山新城	jinshanxincheng
308	获取点击数量	URL	列表页_区域_金山区_朱泾	zhujing
309	获取点击数量	URL	列表页_区域_金山区_亭林	tinglin
310	获取点击数量	URL	列表页_区域_金山区_石化	shihua
311	获取点击数量	URL	列表页_区域_金山区_金山卫镇	jinshanweizhen
312	获取点击数量	URL	列表页_区域_松江区_全松江区	songjiang
313	获取点击数量	URL	列表页_区域_松江区_松江新城	songjiangxincheng
314	获取点击数量	URL	列表页_区域_松江区_九亭	jiuting
315	获取点击数量	URL	列表页_区域_松江区_泗泾	sijing
316	获取点击数量	URL	列表页_区域_松江区_松江老城	songjianglaocheng
317	获取点击数量	URL	列表页_区域_松江区_新桥	xinqiao
318	获取点击数量	URL	列表页_区域_松江区_松江城区	songjiangchengqu
319	获取点击数量	URL	列表页_区域_松江区_莘闵	xinmin
320	获取点击数量	URL	列表页_区域_松江区_佘山	sheshan
321	获取点击数量	URL	列表页_区域_松江区_松江大学城	songjiangdaxuecheng
322	获取点击数量	URL	列表页_区域_松江区_九亭	jiuting1
323	获取点击数量	URL	列表页_区域_松江区_小昆山	xiaokunshan
324	获取点击数量	URL	列表页_区域_松江区_洞泾	dongjing
325	获取点击数量	URL	列表页_区域_青浦区_全青浦区	qingpu
326	获取点击数量	URL	列表页_区域_青浦区_青浦新城	qingpuxincheng
327	获取点击数量	URL	列表页_区域_青浦区_青浦城区	qingpuchengqu
328	获取点击数量	URL	列表页_区域_青浦区_徐泾	xujing
329	获取点击数量	URL	列表页_区域_青浦区_朱家角	zhujiajiao
330	获取点击数量	URL	列表页_区域_青浦区_赵巷	zhaoxiang
331	获取点击数量	URL	列表页_区域_青浦区_白鹤	baihe
332	获取点击数量	URL	列表页_区域_青浦区_华新	huaxin
333	获取点击数量	URL	列表页_区域_奉贤区_全奉贤区	fengxian
334	获取点击数量	URL	列表页_区域_奉贤区_西渡	xidu
335	获取点击数量	URL	列表页_区域_奉贤区_南桥	nanqiao
336	获取点击数量	URL	列表页_区域_奉贤区_奉城	fengcheng
337	获取点击数量	URL	列表页_区域_奉贤区_海湾旅游区	haiwanlvyouqu
338	获取点击数量	URL	列表页_区域_奉贤区_柘林	zhelin
339	获取点击数量	URL	列表页_区域_奉贤区_金汇镇	jinhuizhen
340	获取点击数量	URL	列表页_区域_奉贤区_庄行	zhuanghang
341	获取点击数量	URL	列表页_区域_奉贤区_海湾	haiwan
342	获取点击数量	URL	列表页_区域_崇明县_全崇明县	chongming
343	获取点击数量	URL	列表页_区域_崇明县_崇明	chongming01
344	获取点击数量	URL	列表页_区域_崇明县_长兴岛	changxingdao
345	获取点击数量	URL	列表页_区域_崇明县_堡镇	baozhen
346	获取点击数量	URL	列表页_区域_崇明县_陈家镇	chenjiazhen
347	获取点击数量	URL	列表页_区域_玄武区_全玄武区	xuanwu
348	获取点击数量	URL	列表页_区域_玄武区_珠江路	zhujianglu
349	获取点击数量	URL	列表页_区域_玄武区_北京东路	beijingdonglu
350	获取点击数量	URL	列表页_区域_玄武区_锁金村	suojincun
351	获取点击数量	URL	列表页_区域_玄武区_月苑	yueyuan
352	获取点击数量	URL	列表页_区域_玄武区_卫岗	weigang
353	获取点击数量	URL	列表页_区域_玄武区_长江路	changjianglu
354	获取点击数量	URL	列表页_区域_玄武区_板仓	bancang
355	获取点击数量	URL	列表页_区域_玄武区_红山	hongshan
356	获取点击数量	URL	列表页_区域_玄武区_樱驼花园	yingtuohuayuan
357	获取点击数量	URL	列表页_区域_玄武区_孝陵卫	xiaolingwei
358	获取点击数量	URL	列表页_区域_玄武区_太平门	taipingmen
359	获取点击数量	URL	列表页_区域_玄武区_玄武门	xuanwumen
360	获取点击数量	URL	列表页_区域_玄武区_后宰门	houzaimen
361	获取点击数量	URL	列表页_区域_玄武区_丹凤街	danfengjie
362	获取点击数量	URL	列表页_区域_玄武区_迈皋桥	maigaoqiao
363	获取点击数量	URL	列表页_区域_玄武区_苜蓿园	muxulu
364	获取点击数量	URL	列表页_区域_玄武区_马群	maqun
365	获取点击数量	URL	列表页_区域_玄武区_中山门	zhongshanmen
366	获取点击数量	URL	列表页_区域_玄武区_龙蟠路	longpanlu
367	获取点击数量	URL	列表页_区域_玄武区_北京西路	beijingxilu
368	获取点击数量	URL	列表页_区域_玄武区_江宁大学城	jiangningdaxuecheng
369	获取点击数量	URL	列表页_区域_玄武区_瑞金路	ruijinlu1
370	获取点击数量	URL	列表页_区域_白下区_全白下区	baixia
371	获取点击数量	URL	列表页_区域_白下区_朝天宫	chaotiangong1
372	获取点击数量	URL	列表页_区域_白下区_五老村	wulaocun1
373	获取点击数量	URL	列表页_区域_白下区_大光路	daguanglu1
374	获取点击数量	URL	列表页_区域_白下区_瑞金路	ruijinlu2
375	获取点击数量	URL	列表页_区域_白下区_洪武路	hongwulu1
376	获取点击数量	URL	列表页_区域_秦淮区_全秦淮区	qinghuai
377	获取点击数量	URL	列表页_区域_秦淮区_新街口	xinjiekou
378	获取点击数量	URL	列表页_区域_秦淮区_秦虹	qinhong
379	获取点击数量	URL	列表页_区域_秦淮区_洪武路	hongwulu
380	获取点击数量	URL	列表页_区域_秦淮区_朝天宫	chaotiangong
381	获取点击数量	URL	列表页_区域_秦淮区_建康路	jiankanglu
382	获取点击数量	URL	列表页_区域_秦淮区_大光路	daguanglu
383	获取点击数量	URL	列表页_区域_秦淮区_瑞金路	ruijinlu
384	获取点击数量	URL	列表页_区域_秦淮区_五老村	wulaocun
385	获取点击数量	URL	列表页_区域_秦淮区_常府街	changfujie
386	获取点击数量	URL	列表页_区域_秦淮区_淮海路	huaihailu
387	获取点击数量	URL	列表页_区域_秦淮区_中山南路	zhongshannanlu
388	获取点击数量	URL	列表页_区域_秦淮区_升州路	shengzhoulu
389	获取点击数量	URL	列表页_区域_秦淮区_光华路	guanghualu
390	获取点击数量	URL	列表页_区域_秦淮区_光华门	guanghuamen
391	获取点击数量	URL	列表页_区域_秦淮区_月牙湖	yueyahu
392	获取点击数量	URL	列表页_区域_秦淮区_御道街	yudaojie
393	获取点击数量	URL	列表页_区域_秦淮区_长白街	changbaijie
394	获取点击数量	URL	列表页_区域_秦淮区_太平南路	taipingnanlu
395	获取点击数量	URL	列表页_区域_秦淮区_夫子庙	fuzimiao
396	获取点击数量	URL	列表页_区域_秦淮区_许府巷	xufuxiang
397	获取点击数量	URL	列表页_区域_秦淮区_大行宫	daxinggong
398	获取点击数量	URL	列表页_区域_秦淮区_来凤	laifeng
399	获取点击数量	URL	列表页_区域_秦淮区_洪家园	hongjiayuan
400	获取点击数量	URL	列表页_区域_秦淮区_大明路	daminglu
401	获取点击数量	URL	列表页_区域_秦淮区_钓鱼台	diaoyutai
402	获取点击数量	URL	列表页_区域_秦淮区_长乐路	changlelu
403	获取点击数量	URL	列表页_区域_秦淮区_中华门	zhonghuamen
404	获取点击数量	URL	列表页_区域_秦淮区_马道街	madaojie
405	获取点击数量	URL	列表页_区域_秦淮区_双塘	shuangtang
406	获取点击数量	URL	列表页_区域_秦淮区_集庆路	jiqinglu
407	获取点击数量	URL	列表页_区域_秦淮区_三山街	sanshanjie
408	获取点击数量	URL	列表页_区域_建邺区_全建邺区	jianye
409	获取点击数量	URL	列表页_区域_建邺区_奥体	aoti
410	获取点击数量	URL	列表页_区域_建邺区_应天西路	yingtianxilu
411	获取点击数量	URL	列表页_区域_建邺区_集庆门	jiqingmen
412	获取点击数量	URL	列表页_区域_建邺区_汉中门	hanzhongmen
413	获取点击数量	URL	列表页_区域_建邺区_南苑	nanyuan
414	获取点击数量	URL	列表页_区域_建邺区_湖西街	huxijie
415	获取点击数量	URL	列表页_区域_建邺区_长虹路	changhonglu
416	获取点击数量	URL	列表页_区域_建邺区_云锦路	yunjinlu
417	获取点击数量	URL	列表页_区域_建邺区_兴隆大街	xinglongdajie
418	获取点击数量	URL	列表页_区域_建邺区_南湖	nanhu
419	获取点击数量	URL	列表页_区域_建邺区_茶南	chanan
420	获取点击数量	URL	列表页_区域_建邺区_水西门	shuiximen
421	获取点击数量	URL	列表页_区域_建邺区_莫愁路	mochoulu
422	获取点击数量	URL	列表页_区域_建邺区_万达广场	wandaguangchang
423	获取点击数量	URL	列表页_区域_建邺区_江东	jiangdong
424	获取点击数量	URL	列表页_区域_建邺区_应天大街	yingtiandajie
425	获取点击数量	URL	列表页_区域_建邺区_牡丹	mudan
426	获取点击数量	URL	列表页_区域_建邺区_兴隆	xinglong
427	获取点击数量	URL	列表页_区域_鼓楼区_全鼓楼区	gulou
428	获取点击数量	URL	列表页_区域_鼓楼区_凤凰西街	fenghuangxijie
429	获取点击数量	URL	列表页_区域_鼓楼区_中央门	zhongyangmen
430	获取点击数量	URL	列表页_区域_鼓楼区_上海路	shanghailu
431	获取点击数量	URL	列表页_区域_鼓楼区_挹江门	yijiangmen
432	获取点击数量	URL	列表页_区域_鼓楼区_广州路	guangzhoulu
433	获取点击数量	URL	列表页_区域_鼓楼区_华侨路	huaqiaolu
434	获取点击数量	URL	列表页_区域_鼓楼区_三牌楼	sanpailou
435	获取点击数量	URL	列表页_区域_鼓楼区_黑龙江路	heilongjianglu
436	获取点击数量	URL	列表页_区域_鼓楼区_福建路	fujianlu
437	获取点击数量	URL	列表页_区域_鼓楼区_宁海路	ninghailu
438	获取点击数量	URL	列表页_区域_鼓楼区_定淮门	dinghuaimen
439	获取点击数量	URL	列表页_区域_鼓楼区_清凉门	qingliangmen
440	获取点击数量	URL	列表页_区域_鼓楼区_水佐岗	shuizuogang
441	获取点击数量	URL	列表页_区域_鼓楼区_热河南路	rehenanlu
442	获取点击数量	URL	列表页_区域_鼓楼区_金陵小区	jinlingxiaoqu
443	获取点击数量	URL	列表页_区域_鼓楼区_五塘广场	wutangguangchang
444	获取点击数量	URL	列表页_区域_鼓楼区_建宁路	jianninglu
445	获取点击数量	URL	列表页_区域_鼓楼区_小市	xiaoshi
446	获取点击数量	URL	列表页_区域_鼓楼区_湖南路	hunanlu
447	获取点击数量	URL	列表页_区域_浦口区_全浦口区	quanpukouqu
448	获取点击数量	URL	列表页_区域_浦口区_江浦街道	jiangpujiedao
449	获取点击数量	URL	列表页_区域_浦口区_顶山街道	dingshanjiedao
450	获取点击数量	URL	列表页_区域_浦口区_桥北	qiaobei
451	获取点击数量	URL	列表页_区域_浦口区_泰山街道	taishanjiedao
452	获取点击数量	URL	列表页_区域_浦口区_高新区	gaoxinqu
453	获取点击数量	URL	列表页_区域_栖霞区_全栖霞区	qixia
454	获取点击数量	URL	列表页_区域_栖霞区_仙林	xianlin
455	获取点击数量	URL	列表页_区域_栖霞区_万寿	wanshou
456	获取点击数量	URL	列表页_区域_栖霞区_晓庄	xiaozhuang
457	获取点击数量	URL	列表页_区域_栖霞区_燕子矶	yanziji
458	获取点击数量	URL	列表页_区域_栖霞区_马群	maqun
459	获取点击数量	URL	列表页_区域_栖霞区_迈皋桥	maigaoqiao0
460	获取点击数量	URL	列表页_区域_栖霞区_尧华门	yaohuamen
461	获取点击数量	URL	列表页_区域_雨花台区_全雨花台区	yuhuatai
462	获取点击数量	URL	列表页_区域_雨花台区_熊仁里	xiongrenli
463	获取点击数量	URL	列表页_区域_雨花台区_铁心桥	tiexinqiao
464	获取点击数量	URL	列表页_区域_雨花台区_小行	xiaoxing
465	获取点击数量	URL	列表页_区域_雨花台区_安德门	andemen
466	获取点击数量	URL	列表页_区域_雨花台区_宁南	ningnan
467	获取点击数量	URL	列表页_区域_雨花台区_雨花新村	yuhuaxincun
468	获取点击数量	URL	列表页_区域_雨花台区_洪家园	hongjiayuan
469	获取点击数量	URL	列表页_区域_江宁区_全江宁区	jiangning
470	获取点击数量	URL	列表页_区域_江宁区_岔路口	chalukou
471	获取点击数量	URL	列表页_区域_江宁区_东山镇	dongshanzhen
472	获取点击数量	URL	列表页_区域_江宁区_科学园	kexueyuan
473	获取点击数量	URL	列表页_区域_江宁区_将军大道	jiangjundadao
474	获取点击数量	URL	列表页_区域_江宁区_九龙湖	jiulonghu
475	获取点击数量	URL	列表页_区域_江宁区_铁心桥	tiex1nqiao
476	获取点击数量	URL	列表页_区域_江宁区_百家湖	baijiahu
477	获取点击数量	URL	列表页_区域_江宁区_麒麟镇	qilinzheng
478	获取点击数量	URL	列表页_区域_六合区_全六合区	liuhe
479	获取点击数量	URL	列表页_区域_六合区_六合	liuhe01
480	获取点击数量	URL	列表页_区域_溧水区_全溧水区	lishui
481	获取点击数量	URL	列表页_区域_六合区_溧水	lishui01
482	获取点击数量	URL	列表页_区域_高淳区_全高淳区	gaochun
483	获取点击数量	URL	列表页_区域_高淳区_高淳	gaochun01
484	获取点击数量	URL	列表页_区域_工业园区_全工业园区	gongyeyuanqu
485	获取点击数量	URL	列表页_区域_工业园区_湖东三	hudongsan
486	获取点击数量	URL	列表页_区域_工业园区_湖东二	hudonger
487	获取点击数量	URL	列表页_区域_工业园区_湖东一	hudongyi
488	获取点击数量	URL	列表页_区域_工业园区_湖西	huxi
489	获取点击数量	URL	列表页_区域_工业园区_娄葑	loufeng
490	获取点击数量	URL	列表页_区域_工业园区_东环	donghuan
491	获取点击数量	URL	列表页_区域_工业园区_师惠	shihui
492	获取点击数量	URL	列表页_区域_工业园区_斜塘	xietang
493	获取点击数量	URL	列表页_区域_工业园区_独墅湖	dushuhu
494	获取点击数量	URL	列表页_区域_工业园区_青剑湖	qingjianhu
495	获取点击数量	URL	列表页_区域_工业园区_唯亭	weiting
496	获取点击数量	URL	列表页_区域_工业园区_玲珑	linglong
497	获取点击数量	URL	列表页_区域_工业园区_东沙湖	dongshahu
498	获取点击数量	URL	列表页_区域_高新区_全高新区	gaoxin
499	获取点击数量	URL	列表页_区域_高新区_枫桥	fengqiao
500	获取点击数量	URL	列表页_区域_高新区_科技城	kejicheng
501	获取点击数量	URL	列表页_区域_高新区_浒墅关	xushuguan
502	获取点击数量	URL	列表页_区域_高新区_狮山	shishan
503	获取点击数量	URL	列表页_区域_高新区_阳山	yangshan
504	获取点击数量	URL	列表页_区域_高新区_通安	tongan
505	获取点击数量	URL	列表页_区域_高新区_东渚	dongzhu
506	获取点击数量	URL	列表页_区域_高新区_横塘	hengteng
507	获取点击数量	URL	列表页_区域_太仓市_全太仓市	taicangshi
508	获取点击数量	URL	列表页_区域_张家港市_全张家港市	zhangjiagang
509	获取点击数量	URL	列表页_区域_沧浪区_全沧浪区	canglang
510	获取点击数量	URL	列表页_区域_沧浪区_葑门	fengmen
511	获取点击数量	URL	列表页_区域_沧浪区_南门	nanmen
512	获取点击数量	URL	列表页_区域_沧浪区_双塔	shuangta
513	获取点击数量	URL	列表页_区域_沧浪区_吴门江	wumenjiang
514	获取点击数量	URL	列表页_区域_沧浪区_胥江	xujiang
515	获取点击数量	URL	列表页_区域_沧浪区_友新	youxin
516	获取点击数量	URL	列表页_区域_平江区_全平江区	pingjiang
517	获取点击数量	URL	列表页_区域_平江区_城北街道	chengbeijiedao
518	获取点击数量	URL	列表页_区域_平江区_观前	guanqian
519	获取点击数量	URL	列表页_区域_平江区_娄门	loumen
520	获取点击数量	URL	列表页_区域_平江区_平江路	pingjianglu
521	获取点击数量	URL	列表页_区域_平江区_苏锦街道	sujinjiedao
522	获取点击数量	URL	列表页_区域_平江区_桃花坞	taohuawu
523	获取点击数量	URL	列表页_区域_金阊区_全金阊区	jinchang
524	获取点击数量	URL	列表页_区域_金阊区_白洋湾	baiyangwan
525	获取点击数量	URL	列表页_区域_金阊区_彩香	caixiang
526	获取点击数量	URL	列表页_区域_金阊区_虎丘	huqiu
527	获取点击数量	URL	列表页_区域_金阊区_留园	liuyuan
528	获取点击数量	URL	列表页_区域_金阊区_石路	shilu
529	获取点击数量	URL	列表页_区域_吴中区_全吴中区	wuzhong
530	获取点击数量	URL	列表页_区域_吴中区_长桥	changqiao
531	获取点击数量	URL	列表页_区域_吴中区_城南	chengnan
532	获取点击数量	URL	列表页_区域_吴中区_东山	dongshan
533	获取点击数量	URL	列表页_区域_吴中区_郭巷	guoxiang
534	获取点击数量	URL	列表页_区域_吴中区_光福	guangfu
535	获取点击数量	URL	列表页_区域_吴中区_横泾	hengjing
536	获取点击数量	URL	列表页_区域_吴中区_开发区	kaifaqu
537	获取点击数量	URL	列表页_区域_吴中区_龙西	longxi
538	获取点击数量	URL	列表页_区域_吴中区_甪直	luzhi
539	获取点击数量	URL	列表页_区域_吴中区_临湖	linhu
540	获取点击数量	URL	列表页_区域_吴中区_木渎	mudu
541	获取点击数量	URL	列表页_区域_吴中区_胥口	xukou
542	获取点击数量	URL	列表页_区域_吴中区_西山	xishan
543	获取点击数量	URL	列表页_区域_吴中区_香山	xiangshan
544	获取点击数量	URL	列表页_区域_吴中区_越溪	yuexi
545	获取点击数量	URL	列表页_区域_吴中区_苏苑	suyuan
546	获取点击数量	URL	列表页_区域_吴中区_世茂	shimao
547	获取点击数量	URL	列表页_区域_相城区_全相城区	xiangcheng
548	获取点击数量	URL	列表页_区域_吴江区_全吴江区	wujiang
549	获取点击数量	URL	列表页_区域_常熟市_全常熟市	changshou
550	获取点击数量	URL	列表页_区域_昆山市_全昆山市	kunshan
551	获取点击数量	按钮	详情页_在线咨询_按钮	info_im_m
552	获取点击数量	按钮	详情页_电话预约_按钮	info_phone_m
553	获取点击数量	url	列表页_求租需求	zuke
554	获取点击数量	按钮	求租需求_提交_按钮	zuke_submit_uid
555	获取点击数量	url	列表页_立即委托	form
556	获取点击数量	按钮	立即委托_提交_按钮	yezhu_submit_uid"""
    h = MySQLHelper('bigdata')

    sql = "insert into page_click_buried_point values(%s,%s,%s,%s,%s)"


    args = [x.split('\t') for x in data.split('\n')]

    for i in args:
        print(i)


    result = h.executemany(sql,args)
    print(result)