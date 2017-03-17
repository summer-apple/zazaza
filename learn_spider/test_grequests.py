import grequests


s1 = grequests.Session()
s2 = grequests.Session()
s3 = grequests.Session()

print(s1.__hash__(),s2.__hash__(),s3.__hash__())

rs = [grequests.get('http://www.baidu.com', session=s1),grequests.get('http://www.baidu.com', session=s2),grequests.get('http://www.baidu.com', session=s3)]

resp = grequests.map(rs)
print(resp)