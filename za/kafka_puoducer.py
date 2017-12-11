# coding:utf-8


import time, json,datetime
from pykafka import KafkaClient



# 可接受多个Client这是重点
client = KafkaClient(hosts='127.0.0.1:9092, \
                            127.0.0.1:9093, \
                            127.0.0.1:9094')
# 选择一个topic
topic = client.topics[b'test']
# 创建一个生产者
producer = topic.get_producer()
# 模拟接收前端生成的商品信息

# 生产消息
while True:
    producer.produce(datetime.datetime.now().strftime('%H:%M:%S').encode())
    time.sleep(1)