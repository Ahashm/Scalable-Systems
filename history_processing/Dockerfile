FROM bde2020/spark-python-template:3.3.0-hadoop3.3

COPY example.py /app

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/example.py
ENV SPARK_SUBMIT_ARGS "--packages org.mongodb.spark:mongo-spark-connector:10.0.5"
