#### Simple detection of DDoS
The LogAnalyzer accomplishes the following:
* Reads messages from *apache-log-msg* topic streamed from Kafka. 
* Processes the messages and applies some known DDoS patterns to filter the IP addresses as threats.
* Log entries i.e. are aggregated into time-series rolling/sliding window of 30 seconds.
* Using the rolling/sliding window approach gives us an accurate picture of traffic vs time.
* If traffic exceeds 250000 threshold & status code for more 50% of requests is 503, then we assuming we are under DDOS.
* For any window, if the above condition holds true, capture the most active IP address and write to a file.


**Architecture**
* *Message model object*: ApacheLogEntry (POJO encapsulates the various parameters that Apache log file provides for each request made by an IP) 
* *processor*: Package containing message processing logic.
* *application.yml*: stores kafka consumer configuration properties.  


**Kafka Consumer Commands**
* Consume from beginning:   
 ```./kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic apache-log-msg --from-beginning```
* View topic info:
 ```./kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic apache-log-msg```
* View consumer group info:
 ```./kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group groupId --describe```

