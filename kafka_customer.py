import time, json
from pykafka import KafkaClient
# 可接受多个Client这是重点
client = KafkaClient(hosts='127.0.0.1:9092, \
                            127.0.0.1:9093, \
                            127.0.0.1:9094')
# 选择一个topic
topic = client.topics[b'test']
# 生成一个消费者
balanced_consumer = topic.get_balanced_consumer(
  consumer_group=b'goods_group',
  auto_commit_enable=True,
  zookeeper_connect='127.0.0.1:2181'
)
# 消费信息
for message in balanced_consumer:
  if message is not None:
    # 解析json为dict
    goods_dict = message.value.decode()
    print(goods_dict)