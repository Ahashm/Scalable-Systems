from kafka import KafkaConsumer
from hdfs import InsecureClient

# Set up the Kafka consumer
consumer = KafkaConsumer("commits",
                         bootstrap_servers=["kafka-server1:9092", "kafka-server2:9092"],
                         value_deserializer=lambda x: x.decode("utf-8"))

# Set up the HDFS client
client = InsecureClient("http://namenode:50070", user="hdfs")

# Consume and write data from the Kafka topic to HDFS
for message in consumer:
    client.write("/my/hdfs/path/data.txt", message.value, append=True)
