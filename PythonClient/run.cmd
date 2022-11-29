docker build . -t pythonclient
docker run -it --network=shared_network pythonclient