#### Simple detection of DDoS
The ProjectChallenge-Processor accomplishes the following:
* Reads messages from *apache-log-msg* topic streamed from Kafka. 
* Processes the messages and applies some known DDoS patterns to filter the IP addresses as threats.
* The filtered IP addresses can then be further processed for further evaluation and analysis. 
* These processes steps are achieved within the following time range: ()

**Architecture**
* *Message model object*: ApacheLogTemplate (POJO encapsulates the various parameters that Apache log file provides for each request made by an IP) 
* *processor*: Package containing message processing logic.
* *application.yml*: stores kafka consumer configuration properties.  


**Kafka Consumer Commands**
* Consume from beginning:   
 ./kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic apache-log-msg --from-beginning ^C
* View topic info:
 ./kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic apache-log-msg
* View consumer group info:
 ./kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group groupId --describe

