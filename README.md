# Kafka-Keycloak Demo  
This repository contains producer and consumer demo for Kafka. 
It uses Python module for Confluent Kafka and uses Keycloak for authentication and authorization. 
Avro schema is used for the topic and the schema is registered with the schema registry. 

## How to use
- Create a virtual environment using `python -m venv env`
- Activate the python virtual environment `source env/bin/activate`
- Install the necessary python modules using `pip install -r requirements.txt` 
- Create `config.py` from `config_template.py` and update the configurations in the file. 
- Run the producer using `python avro_producer.py <topic_name>`
- Run the consumer using `python avro_consumer.py <topic_name>`