import requests
import time
import datetime

TOUCH_URL = 'http://www.epicc.com.cn/chexian/'
t = int(time.mktime((datetime.datetime.now() + datetime.timedelta(hours=3)).timetuple()) * 1000)
PRE_PRICE_URL = 'http://www.epicc.com.cn/newecar/proposal/preForCalBI?time=%s' % t  # int(time.time()*1000)
# http://www.epicc.com.cn/newecar/calculate/initKindInfo      # uniqueID	ecbef7c6-ef52-41de-86c2-9cdff3cbf253

PRICE_URL = 'http://www.epicc.com.cn/newecar/calculate/calculateForBatch?time=%s' % t

headers = [
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

with open('para.txt', 'rb') as f:
    param = dict()

    for l in f.readlines():
        lst = l[:-1].decode().split('\t')
        if len(lst) == 1:
            param[lst[0]] = ''
        else:
            param[lst[0]] = lst[1]

    print(param)

sn = requests.session()
sn.headers = headers
resp = sn.get(TOUCH_URL)

# with open('touch.html','wb') as f:
#     f.write(resp.content)

resp = sn.post(PRE_PRICE_URL, data=param)
j = resp.json()

print(j['resultMsg'])

'''
{"elementID":"","platFormModelCode":"","resultCode":"0000","resultFlag":true,
"resultMsg":"算费前校验成功","resultUrl":"","uniqueID":""}
'''

with open('para2.txt', 'rb') as f:
    param2 = dict()

    for l in f.readlines():
        lst = l[:-1].decode().split('\t')
        if len(lst) == 1:
            param2[lst[0]] = ''
        else:
            param2[lst[0]] = lst[1]

    print(param2)

resp2 = sn.post(PRICE_URL, data=param2)
j = resp2.json()

biPremiumMessage = j['biPremiumMessage']
prices = biPremiumMessage['packageCombos'][0]['items']

total_price = 0

for i in prices:
    code = i['kindCode']
    price = i['premium']
    print(code, price)
    total_price += price

print(total_price)

carShipTax = j['carShipTaxMessage']['thisPayTax']

ciPremium = j['ciPremiumMessage']['premium']

print('carShipTax', carShipTax)
print('ciPremium', ciPremium)