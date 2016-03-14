import time
from kafka import KafkaConsumer

consumer = KafkaConsumer('test', group_id='my-group',bootstrap_servers=['localhost:2181'])

fHandle = open("E:\\BetterHack\\iislogs\\Log.txt")
count = 0
#Start time
print (time.time())

for message in consumer:
    count = count + 1

#End Time
print (time.time())
print (count)
fHandle.close()

