from kafka import KafkaConsumer
from kafka import TopicPartition
from threading import Thread

def func(topic, partition):
  i=0
  consumer = KafkaConsumer(bootstrap_servers='104.154.53.184:6667', group_id='grp-5327', auto_offset_reset='earliest', 
  consumer_timeout_ms = 10000)
  consumer.assign([TopicPartition(topic, partition)])
  for msg in consumer:
    i=i+1
  print(i)

thread0 = Thread(target = func, args = ('small', 0))
thread1 = Thread(target = func, args = ('small', 1))
thread2 = Thread(target = func, args = ('small', 2))
thread3 = Thread(target = func, args = ('small', 3))

thread0.daemon = True
thread1.daemon = True
thread2.daemon = True
thread3.daemon = True

thread0.start()
thread1.start()
thread2.start()
thread3.start()
thread0.join()

thread1.join()
thread2.join()
thread3.join()

