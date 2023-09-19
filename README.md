# DBT Mini Project

## Real time Data Streaming and Visualization using Spark and Kafka

<table>
    <tr>
    	<th>SRN</th>
        <th>Name</th>
    </tr>
    <tr>
    	<td>PES1UG20CS020</td>
        <td>Aditya Mahesh</td>
    </tr>
    <tr>
    	<td>PES1UG20CS042</td>
        <td>Ananya Jalan</td>
    </tr>
    <tr>
    	<td>PES1UG20CS084</td>
        <td>Aryansh Bhargavan</td>
    </tr>
    <tr>
    	<td>PES1UG20CS093</td>
        <td>Avni Gupta</td>
    </tr>
</table>



### Setup/Usage

- Start Zookeeper

```
zookeeper-server-start.sh kafka\config\zookeeper.properties
```
* Start the Kafka server
```
kafka-server-start.sh kafka\config\server.properties
```
* Create the required topics in Kafka
```
kafka-topics.bat --create --bootstrap-server localhost:2181 --replication-factor 1 --partitions 1 --topic weather
kafka-topics.bat --create --bootstrap-server localhost:2181 --replication-factor 1 --partitions 1 --topic output
```
* Execute the producer.py program. This will take the data from the API and start publishing to the Kafka topic "weather".
```
python producer.py
```
* Start the consumer using the Spark-Submit. This will start processing the data using Spark Structured Streaming and send the output to the Kafka topic "output".
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumer.py
```
* Execute the output.py program. This will take the data from the Kafka topic "output" and visualize it.
```
python output_stream.py     # for stream data
python output_batch.py      # for batch data 
```

