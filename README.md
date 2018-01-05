# Steps to run the example

- From the project root, execute:
docker-compose -f docker/docker-compose.yml up -d

- Import the example data running:
docker exec -it mongo_container sh /scripts/import-data.sh

- Import the code as a Maven project.
