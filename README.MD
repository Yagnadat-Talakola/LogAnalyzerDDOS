#### Simple detection of DDoS
The LogAnalyzer accomplishes the following:
* Reads messages from *apache-log-msg* topic streamed from Kafka. 
* Processes the messages and applies some known DDoS patterns to filter the IP addresses as threats.
* Log entries are aggregated into time-series rolling window of 30 seconds duration
* I decided to capture the ipAddresses in a time-series rolling window structure because it gives me a better picture of traffic vs time.
* Also accuracy might improve if we narrow down the rolling window duration to smaller window.

**Assumptions**
* Let's assume my server can handle max of 830 req/second
* *Simple approach:*
  If traffic exceeds 25000 (= 830 * 30) request volume in a rolling window of 30 seconds, then I am assuming DDOS. Also checking if the status code is 503 for additional data point.
* For better results, it is better to analyze traffic patterns in each window and identify anomalies using Machine Learning techniques.

**Architecture**
* *Message object*: ApacheLogEntry (POJO encapsulates the various parameters that Apache log file provides for each request made by an IP) 
* *processor*: Package containing message processing logic.
* *application.yml*: stores kafka consumer configuration properties.  


**Kafka Consumer Commands**
* Consume from beginning:   
 ```./kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic apache-log-msg --from-beginning```
* View topic info:
 ```./kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic apache-log-msg```
* View consumer group info:
 ```./kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group groupId --describe```

