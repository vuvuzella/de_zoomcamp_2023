## Create a topic in the broker container:
1. `docker compose exec -it kafka-broker /bin/bash`
2. `kafka-topics --create --topic rides-json --bootstrap-server kafka-broker:29092 --replication-factor 1 --partitions 1`
3. Run your producer to start publishing to the topic
