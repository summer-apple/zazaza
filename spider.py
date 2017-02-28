import requests
import time
import datetime
import uuid
import param


class Spider:


    TOUCH_URL = 'http://www.epicc.com.cn/newecar/uuid/initUUidCar'

    MODEL_URL = 'http://www.epicc.com.cn/newecar/car/findCarModelJYQuery'




    t = int(time.mktime((datetime.datetime.now() + datetime.timedelta(hours=3)).timetuple()) * 1000)
    PRE_PRICE_URL = 'http://www.epicc.com.cn/newecar/proposal/preForCalBI?time=%s' % t  # int(time.time()*1000)
    # http://www.epicc.com.cn/newecar/calculate/initKindInfo      # uniqueID	ecbef7c6-ef52-41de-86c2-9cdff3cbf253

    PRICE_URL = 'http://www.epicc.com.cn/newecar/calculate/calculateForBatch?time=%s' % t

    HEADERS = [
        ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
        ('Accept-Encoding', 'gzip, deflate, sdch, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4'),
        ('Cache-Control', 'max-age=0'),
        ('Connection', 'keep-alive'),
        ('Host', 'www.zhihu.com'),
        ('Upgrade-Insecure-Requests', 1),
        ('User-Agent',
         'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36')
    ]

    def __init__(self,plate_no,model,vin,engine_no,register_date,issue_date,ownner,id_card,mobile,email=''):
        self.sn = requests.session()
        self.sn.headers = Spider.HEADERS
        self.uniqueID = str(uuid.uuid1())

        self.plate_no = plate_no
        self.model = model
        self.vin = vin
        self.engine_no = engine_no
        self.register_date = register_date
        self.issue_date = issue_date
        self.ownner = ownner
        self.id_card = id_card
        self.mobile = mobile
        self.email = email


    def touch(self):
        touch_param = {'vehicleModelsh':'', 'uniqueID':self.uniqueID}
        touch_resp = self.sn.post(Spider.TOUCH_URL,data=touch_param)
        print(touch_resp.status_code)


    def qry_model(self):
        qry_param = Spider.init_param(param.model)
        qry_param['uniqueId'] = self.uniqueID
        qry_param['carModelJYQuery.vehicleName'] = self.model

        resp = self.sn.post(Spider.MODEL_URL, data=qry_param).json()

        if resp['head']['errorCode'] == '0000': # 普通车辆查询
            model = resp['body']['queryVehicle'][0]
            return model['vehicleName']
        else:
            # 作为进口车查询
            qry_param['carModelJYQuery.vehicleName'] = Spider.only_chinese_character(qry_param['carModelJYQuery.vehicleName'])
            resp2 = self.sn.post(Spider.MODEL_URL, data=qry_param).json()

            if resp2['head']['errorCode'] == '0000':
                model = resp2['body']['queryVehicle'][0]
                return model['vehicleName']
            else:
                return ''









    def pre_for_bi(self):
        data = Spider.init_param(param.pre_for_bi)
        data['uniqueID'] = self.uniqueID

        data['licenseNo'] = self.plate_no
        data['carReqDto.frameno'] = self.vin
        data['carReqDto.engineno'] = self.engine_no
        data['carReqDto.enrolldate'] = self.register_date
        data['carReqDto.vehicle_modelsh'] = self.qry_model()
        data['insuredReqDtos[1].insuredname'] = self.ownner
        data['insuredReqDtos[1].identifyno'] = self.id_card
        data['insuredReqDtos[1].mobile'] = self.mobile
        data['insuredReqDtos[1].email'] = self.email


        resp = self.sn.post(Spider.PRE_PRICE_URL, data=data)
        j = resp.json()

        print(j['resultMsg'])



    def get_all_price(self):

        data = Spider.init_param(param.get_all_price)
        data['uniqueID'] = self.uniqueID


        resp2 = self.sn.post(Spider.PRICE_URL, data=data)
        j = resp2.json()

        biPremiumMessage = j['biPremiumMessage']
        prices = biPremiumMessage['packageCombos'][0]['items']

        total_price = 0

        for i in prices:
            code = i['kindCode']
            price = i['premium']
            print(code, price)
            total_price += price

        carShipTax = j['carShipTaxMessage']['thisPayTax']
        ciPremium = j['ciPremiumMessage']['premium']



        print('biPremium', total_price)
        print('carShipTax', carShipTax)
        print('ciPremium', ciPremium)



# -----------——-------------------- Utils -----------------------------------

    @staticmethod
    def is_chinese(uchar):
        """判断一个unicode是否是汉字"""
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False

    @staticmethod
    def remove_chinese_character(mix_str):
        tmp = ''
        for i in mix_str:
            if not Spider.is_chinese(i) and i not in [')', '(']:
                tmp += i
        return tmp

    @staticmethod
    def only_chinese_character(mix_str):
        tmp = ''
        for i in mix_str:
            if Spider.is_chinese(i) and i not in [')', '(', '）', '（', '牌']:
                tmp += i
        return tmp

    @staticmethod
    def init_param(param_str):
        p_lst = param_str.split('\n')
        kv_param = dict()

        for p in p_lst:
            lst = p.split('\t')

            if lst[0] != '':
                if len(lst) == 1:
                    kv_param[lst[0]] = ''
                else:
                    kv_param[lst[0]] = lst[1]
        return kv_param


if __name__ == '__main__':
    spider = Spider(plate_no='浙A8ST85',
                    model='指南者',
                    vin='1C4NJDCB2Fd167839',
                    engine_no='FD167839',
                    register_date='2015/04/16',
                    issue_date='2105/04/16',
                    ownner='啦啦啦',
                    id_card='330621199209263517',
                    mobile='15757135743',
                    email='')

    print(spider.uniqueID)

    spider.touch()

    # data = Spider.init_param(param.model_list)
    # data['uniqueID'] = spider.uniqueID
    # data['carModelJYQuery.vehicleName'] = Spider.only_chinese_character(spider.model)
    #model = spider.sn.post(Spider.MODEL_LIST_URL,data=data)


    spider.pre_for_bi()

