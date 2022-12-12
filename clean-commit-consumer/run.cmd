docker build . -t clean-commit-consumer:latest 
docker run --rm -e ENABLE_INIT_DAEMON=false --network big-data-network --name clean_commit_consumer clean-commit-consumer