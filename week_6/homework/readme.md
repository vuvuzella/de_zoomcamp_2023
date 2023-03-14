## Week 6 Homework

1. Select Statements that are correct:
- Kafka Node is responsible to store topics (true)
- Zookeeper is removed from Kafka starting from version 4.0 (dunno)
- Retention configuration ensures that messages are not lost over specific period of time (false? retention forever until persistent store is destroyed)
- Group-Id ensures the messages are distributed to associated consumers (False, it ensures messages are distributed over a topic's partitions)

2. Please select the kafka concepts that support reliability and availability:
- Topic Replication (select, promotes Reliability, using a RAFT algorithm)
- Topic Partitioning
- Consumer Group Id (select, https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/ )
- Ack All   (select, guarantees message are replicated to all replicas)

3. Please select the kafka concepts that support scaling
- Topic Replication
- Topic Partitioning (select, for availability. partitions are needed to horizontally scale consumers, thus providing HA)
- Consumer Group Id
- Ack All

4. Please select the attributes that are food candidates for partitioning key. Consider Cardinality of the field you have selected and scaling aspects of your application
- payment_type  (select)
- vendor_id     (select)
- passenger_count
- total_amount
- tpep_pickup_datetime  (select)
- tpep_dropoff_datetime (select)

5. Which configurations below should be provided for kafka Consumer but not needed for Kafka Producer
- Deserializer Configuration    (select)
- Topics Subscription   (select)
- Bootstrap Server
- Group-Id
- Offset    (select)
- Cluster Key and Cluster-Secret


Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets

Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
