# Kafka_on_Spark
This is part of the Final Project of Data Storage and Retrieval course for<br/>
Master of Information and Data Science<br/>University of California, Berkeley

<ul>
<li>
Data_Producer.scala: Simulate multiple stream data sources using Spark.  Each Spark distributed process generates one Kafka producer which sends message into the Kafka cluster.  The producer is closed once the data stream is exhausted in each distributed process.
</li>
<li>
mongo_db_data_persist.py: Persisting data into MongoDB, using date as collection partition.  Data is persisted in append-only immutable fashion.
</li>
<ul>
