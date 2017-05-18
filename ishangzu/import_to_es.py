from elasticsearch import Elasticsearch
import uuid

es = Elasticsearch(host='localhost',port=9200)

def init_data():
    with open('reports/house.csv','rb') as f:
        lines = f.readlines()

    lines.pop(0).decode()
    for l in lines:

        vals = l.decode()[:-2].split(',')

        if len(vals) != 23:
            continue


        d = dict()
        d['res_id'] = vals[0]
        #d['res_type'] = vals[1]
        #d['pc_detail_url'] = vals[2]
        #d['m_detail_url'] = vals[3]
        d['city_name'] = vals[4]
        d['area_name'] = vals[5]
        d['business_circle_name'] = vals[6]
        d['residential_name'] = vals[7]
        d['property_address'] = vals[8]
        d['house_type'] = vals[9]
        #d['build_area'] = vals[10]
        d['orientation'] = vals[11]
        #d['floor'] = vals[12]
        #d['fitment_type'] = vals[13]
        d['price'] = float(vals[14])
        #d['apartment_type'] = vals[15]
        d['rent_type'] = vals[16]
        #d['audit_status'] = vals[17]
        #d['audit_time'] = vals[18]
        #d['wbo_call_count'] = int(vals[19])
        #d['wbo_mobile_call_count'] = int(vals[20])
        #d['house_room_features'] = vals[21]
        d['datestr'] = vals[22]

        if d['city_name'] == '上海市':
            index = 'sh'
        elif d['city_name'] == '南京市':
            index = 'nj'
        elif d['city_name'] == '苏州市':
            index = 'sz'
        else:
            index = 'hz'

        if '/zheng/' in vals[3]:
            type = 'zheng'
        else:
            type = 'he'





        es.create(index=index,doc_type=type,body=d,id=d['res_id'])



def qry():
    index = 'hz'
    type = 'he'
    doc ={
              "query": {
                "bool": {
                    "filter": {"range": {"price": {"gt":2000}},"weight":3},
                      "must": [{"match": {"area_name": "萧山区"}},{"match": {"residential_name": "广元公寓"}}],
                      "must_not":{"match":{"orientation": "南"}},
                }
              }
            }

    result = es.search(index,type,doc,size=100)

    for r in result['hits']['hits']:
        print(r)



qry()