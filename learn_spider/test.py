import re,requests

"""
iplist = [] ##初始化一个list用来存放我们获取到的IP
html = requests.get("http://haoip.cc/tiqu.htm")##不解释咯
iplistn = re.findall(r'r/>(.*?)<b', html.text, re.S) ##表示从html.text中获取所有r/><b中的内容，re.S的意思是包括匹配包括换行符，findall返回的是个list哦！
for ip in iplistn:
    i = re.sub('\n', '', ip)##re.sub 是re模块替换的方法，这儿表示将\n替换为空
    iplist.append(i.strip()) ##添加到我们上面初始化的list里面, i.strip()的意思是去掉字符串的空格哦！！（这都不知道的小哥儿基础不牢啊）
    print(i.strip())
print(iplist)
"""

iplist = ['211.143.45.216:3128', '111.1.3.38:8000', '124.192.252.165:3128', '221.226.82.130:8998', '60.194.11.179:3128',
          '60.194.183.251:3128', '119.29.232.113:3128', '218.76.106.78:3128', '219.138.139.174:8000', '60.206.201.126:3128',
          '124.193.33.233:3128', '124.88.67.23:843', '124.88.67.24:843', '60.206.202.248:3128', '124.192.106.247:3128',
          '220.248.229.45:3128', '202.106.16.36:3128', '58.210.202.234:808', '60.194.234.213:3128', '60.207.239.245:3128',
          '218.67.126.15:3128', '203.91.121.74:3128', '60.191.164.83:3128', '60.194.46.119:3128', '124.207.132.242:3128',
          '221.211.110.34:3128', '60.160.34.4:3128', '210.101.131.229:8080', '60.194.46.118:3128']

set()
for i in iplist:
    proxies = {'http': i}
    try:
        resp = requests.get('http://45.78.3.33:5000', proxies= proxies,timeout=5)
        print(resp.status_code)
    except Exception as e:
        continue

