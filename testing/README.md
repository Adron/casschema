# What's This "Testing" Folder About?!

To run tests for this application, I've decided that I'd set them to run as integration
tests against an Apache Cassandra node running at localhost. I'm using Docker but you could
easily swap out the default Apache Cassandra that runs locally via a Linux install for example.

The example container I'm using is the Bitnami Apache Cassandra v3 Container. It's located here:
[Bitnami Apache Cassandra Image](https://hub.docker.com/r/bitnami/cassandra/).

The container is launched via the docker-compose.yaml file here in this directory.

Bring up the node for dev/test.

`docker-compose up -d`

Bring down the node.

`docker-compose down`
