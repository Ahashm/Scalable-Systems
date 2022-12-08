FROM confluentinc/cp-server-connect-base:7.3.0

RUN   confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.8.0 \
   && confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:10.1.14