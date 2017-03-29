import grequests
import time,random


un_download_urls = list()
faild_urls = list()

with open('result.txt','r') as f:
    for l in f.readlines():

        name_author_url, tags = l[1:-3].split(', [')
        name,author,url = name_author_url.replace('\'','').replace(' ','').split(',')
        tags = tags.replace('\'','').replace(' ','').split(',')

        # print(name)
        # print(author)
        #print(url)
        # print(tags)
        #print(name_author_url)

        un_download_urls.append(url)


un_download_urls = ['http://a4.topitme.com/o155/101555810175417887.jpg', 'http://a4.topitme.com/l071/100710505699d41b52.jpg', 'http://a4.topitme.com/o168/1016851784c7e864cb.jpg', 'http://a4.topitme.com/o104/10104853530a1892d2.jpg', 'http://a4.topitme.com/o141/10141130683f236317.jpg', 'http://a4.topitme.com/o147/10147791824e49146f.jpg', 'http://a4.topitme.com/o/201010/24/12879152217295.jpg', 'http://a4.topitme.com/o/201103/06/12993979426385.jpg', 'http://a4.topitme.com/l/201009/07/12838635336348.jpg', 'http://a4.topitme.com/o/201102/16/12978692142276.jpg', 'http://a4.topitme.com/o047/1004705164921fe868.jpg', 'http://a4.topitme.com/o146/101469597310dcdd98.jpg', 'http://a4.topitme.com/o144/10144242118fcb29c7.jpg', 'http://a4.topitme.com/l/201004/13/12711203137199.jpg', 'http://a4.topitme.com/o/201005/11/12735489786040.jpg', 'http://a4.topitme.com/o/201007/25/12800246389333.jpg', 'http://a4.topitme.com/o125/101252978864fa8764.jpg', 'http://a4.topitme.com/o045/1004513271d5da7be7.jpg', 'http://a4.topitme.com/o108/101080655074f38524.jpg', 'http://a4.topitme.com/o/201103/10/12997584756143.jpg', 'http://a4.topitme.com/o/201101/06/12943196539192.jpg', 'http://a4.topitme.com/l029/1002998817421890d1.jpg', 'http://a4.topitme.com/o093/100938857164b36094.jpg', 'http://a4.topitme.com/o097/1009767963d2da492b.jpg', 'http://a4.topitme.com/l149/1014952948c076dc65.jpg', 'http://a4.topitme.com/l/201102/17/12979257185694.jpg', 'http://a4.topitme.com/o/201003/28/12697743765031.jpg', 'http://a4.topitme.com/o048/100486113387a4a6d7.jpg', 'http://a4.topitme.com/l/201102/14/12976477861862.jpg', 'http://a4.topitme.com/o/201102/14/12976477873099.jpg', 'http://a4.topitme.com/l/201102/14/12976477894293.jpg', 'http://a4.topitme.com/l/201102/14/12976521485784.jpg', 'http://a4.topitme.com/l/201102/14/12976531063263.jpg', 'http://a4.topitme.com/l/201102/14/12976531181605.jpg', 'http://a4.topitme.com/o/201102/14/12976539096737.jpg', 'http://a4.topitme.com/o/201102/14/12976539876799.jpg', 'http://a4.topitme.com/o/201102/14/12976541286998.jpg', 'http://a4.topitme.com/l/201102/17/12979218965433.jpg', 'http://a4.topitme.com/o/201102/14/12976548555259.jpg', 'http://a4.topitme.com/l/201102/04/12967930888613.jpg', 'http://a4.topitme.com/o/201102/04/12967500885319.jpg', 'http://a4.topitme.com/o151/1015198119d6414d24.jpg', 'http://a4.topitme.com/l086/10086726896fe62bdf.jpg', 'http://a4.topitme.com/o103/10103832925f43d2a6.jpg', 'http://a4.topitme.com/o064/10064071958b08d4e4.jpg', 'http://a4.topitme.com/l168/1016851901bbea0173.jpg', 'http://a4.topitme.com/o/201006/22/12772214843657.jpg', 'http://a4.topitme.com/l/201005/02/12727811897807.jpg', 'http://a4.topitme.com/l/201012/29/12936048749593.jpg', 'http://a4.topitme.com/l109/10109841581679890b.jpg', 'http://a4.topitme.com/l/201103/03/12991213171216.jpg', 'http://a4.topitme.com/l/201011/13/12896628679877.jpg', 'http://a4.topitme.com/o113/1011334035debc10f8.jpg', 'http://a4.topitme.com/l/201011/08/12892082947932.jpg', 'http://a4.topitme.com/o142/10142426936a3bfd4b.jpg', 'http://a4.topitme.com/l123/101237038531f18399.jpg', 'http://a4.topitme.com/o116/101162265933cfc389.jpg', 'http://a4.topitme.com/o074/10074428783762aeae.jpg', 'http://a4.topitme.com/o122/101226571058fd7f6e.jpg', 'http://a4.topitme.com/l042/1004261419682155b1.jpg', 'http://a4.topitme.com/l053/1005360457e2bdb45a.jpg']

def download(urls):
    faild_urls = list()
    rs = (grequests.get(u) for u in urls)
    resp = grequests.map(rs)

    for r in resp:
        print(r)
        if r is None or r.status_code != 200:
            faild_urls.append(r.request.url)
            continue
        file_path = './media/'+r.request.url.split('/')[-1]
        print(file_path)
        with open(file_path,'wb') as f:
            print(r.status_code)
            f.write(r.content)

    if len(faild_urls) != 0:
        print(len(faild_urls))

        time.sleep(random.random()*10)
        return download(faild_urls)



download(un_download_urls)