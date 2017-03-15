# coding':'utf-8
import requests

LOGIN_URL = 'https://login.taobao.com/member/login.jhtml'
UA = '089#6o5vilvOv2TvyJ+evvvvv964Zx55+59BQfvt3IsRldvVOmvo5y9aEwKo3m45+/yH+fpMZ8p5lP9pZuL0/9++Dp/o3fC9Lu8+v+uMX/p9lJsCOPovypleYeYY+8KOYB8+v+uMO/p9lLpCOPovZ5ZhYK7goZKOYNK+vvkqCtVN3v5vR4eD5GlNhn2oQKvAP+oAydtKAFEPhw5K9Wb6vJ5v+cGqMcvjvlvOe7PKPY0Gbn98bC1Dv45z+ZRtyfIWkLM9NWADvlvhOa/z81PUHDY6aljvwcRtnv5vVQrwpRa0iZnVXeN+vv7AEppLIEa6NM4Dvlv6Wnpk8Da+HDc4nv5vVQrwFf5wyDnVVuv+v+8w/uN7dNMqsPN1OTnl/QeQaR5bQU+/vJ5v+cGqMcV0vlvsl5vgCdezrqbD4Bk4lw/4p/NTvlv0+l/Z9ODBEdM9+lSa2Byfmv/9VjknZPKd+l/csiDBDm09+lSa2ByH3XN+vvWSj7rDyxVJvlvK2OQKFtE/N7fNvlx+pG0T6cRPs5vTaJMTMBpFQuTF/x5wz791bs2c6L/IppIw/9erMBpFQuTFudOPy9Nh6/+vYW8N/IlV/RLpMWE5la6v5kKb5vK9ekZ+ZodQE7KdQI5BStespxKD/x5wz791bs2c6LVQ+lKsep8PzBvpv46l8JAPZ9wDhTwlEe88vn8t/7DmcOr1su+8/vgHSwpE6v2Z3xllvJpTK7Dny+vV+QlJsFDQz7gb87KDY2O/vl7AQs5mR6/5+F7cVnAJZv7X8Wj+vvWnKca4V5Xbvlv5Z/jYxB8+v+uMX/p9kcLCOPovZ5ZhYK7goZKOYB8+v+uMO/p9/ILCOPovypleYeYY+8KOYB8+v+uMX/p9342COPovypleYeYY+8KOYBJ+v+24F+hTqmriDpd0LENV8kT5AM/n1oSYRNF+ANK+vvkqCtVNyJ5v93o2s//i8RUz15vwp+T2T009JL0z'
PASSWORD2 = '98babe3b990c9a88280d94180d6364b5c46f81b6e03099e69861bed17b9e2104cb872bd68f3fad54c86f67bbc01c8cd55cd82ce31e2aefbd40b0e857b5ffb5ca0bdef498f59ac0065ef00d217439ccc5637c833851020ba7b4cf59c63eb4b95d12609341e7c9b09ff5f86ec189246ddb32202a3d75f922821a9f5d9e742f9835'


param = {
    'TPL_username':'潦草的夏夜0',
    'TPL_password':'',
    'ncoSig':'',
    'ncoSessionid':'',
    'ncoToken':'3ba889ad601c213521d7a972226c0ebb006c3f47',
    'slideCodeShow':'false',
    'useMobile':'false',
    'lang':'zh_CN',
    'loginsite':'0',
    'newlogin':'0',
    'TPL_redirect_url':'http://buyertrade.taobao.com/trade/itemlist/list_bought_items.htm?spm=a21bo.50862.1997525045.2.qFe9Z6',
    'from':'tb',
    'fc':'default',
    'style':'default',
    'css_style':'',
    'keyLogin':'false',
    'qrLogin':'true',
    'newMini':'false',
    'newMini2':'false',
    'tid':'',
    'loginType':'3',
    'minititle':'',
    'minipara':'',
    'pstrong':'',
    'sign':'',
    'need_sign':'',
    'isIgnore':'',
    'full_redirect':'',
    'sub_jump':'',
    'popid':'',
    'callback':'',
    'guf':'',
    'not_duplite_str':'',
    'need_user_id':'',
    'poy':'',
    'gvfdcname':'',
    'gvfdcre':'',
    'from_encoding':'',
    'sub':'',
    'TPL_password_2':PASSWORD2,
    'loginASR':'1',
    'loginASRSuc':'1',
    'allp':'',
    'oslanguage':'en-US',
    'sr':'1920*1080',
    'osVer':'',
    'naviVer':'chrome|51.0270463',
    'osACN':'Mozilla',
    'osAV':'5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.63 Safari/537.36',
    'osPF':'Linux x86_64',
    'miserHardInfo':'',
    'appkey':'',
    'nickLoginLink':'',
    'mobileLoginLink':'https://login.taobao.com/member/login.jhtml?redirectURL=http://buyertrade.taobao.com/trade/itemlist/list_bought_items.htm?spm=a21bo.50862.1997525045.2.qFe9Z6&useMobile=true',
    'showAssistantLink':'',
    'um_token':'HV01PAAZ0ab8131c3cbf198258c2050b001a089f',
    'ua':UA
}
header = [
        ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
        ('Accept-Encoding', 'gzip, deflate, sdch, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4'),
        ('Cache-Control', 'max-age=0'),
        ('Connection', 'keep-alive'),
        ('Host', 'www.zhihu.com'),
        ('Upgrade-Insecure-Requests', 1),
        ('User-Agent','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36')
         ]


sn = requests.Session()
sn.headers = header

resp = sn.post(url=LOGIN_URL, data=param)

print(resp.status_code)
with open('login_page.html','wb') as f:
    f.write(resp.content)
