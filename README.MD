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