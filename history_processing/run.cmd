docker build . -t history-processing:latest 
docker run --rm -e ENABLE_INIT_DAEMON=false --network big-data-network --name pyspark history-processing