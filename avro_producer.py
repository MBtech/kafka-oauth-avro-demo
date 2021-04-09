from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import pandas as pd 
import functools
import logging
from keycloak import KeycloakOpenID
import time 
from config import * 
import sys

topic_name = sys.argv[1]

oidc_obj = KeycloakOpenID(server_url=keycloak_url,
                    client_id=client_id,
                    realm_name=realm_name,
                    client_secret_key=client_secret_key)


value_schema_str = """
{
	"namespace": "traffic.avro",
	"type": "record",
	"name": "value",
	"fields": [
		{"name": "date", "type": "string"},
		{"name": "source",  "type": "int"},
		{"name": "target", "type": "int"},
		{"name": "nflows", "type": "int"}
	]
}
"""

key_schema_str = """
{
   "namespace": "traffic.avro",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)

def _get_token(args, config):
    """Note here value of config comes from sasl.oauthbearer.config below.
    It is not used in this example but you can put arbitrary values to
    configure how you can get the token (e.g. which token URL to use)
    """
    # payload = {
    #     'grant_type': 'client_credentials',
    #     'scope': ' '.join(args.scopes)
    # }
    # resp = requests.post(args.token_url,
    #                      auth=(args.client_id, args.client_secret),
    #                      data=payload)
    # token = resp.json()
    # return token['access_token'], time.time() + float(token['expires_in'])
    username = args["username"]
    password = args["password"]
    token = oidc_obj.token(username, password)
    # print(token)
    return token['access_token'], time.time() + float(token['expires_in'])


def producer_config(args):
    logger = logging.getLogger(__name__)
    return {
        'bootstrap.servers': brokers,
        'security.protocol': 'sasl_plaintext',
        'sasl.mechanisms': 'OAUTHBEARER',
        # sasl.oauthbearer.config can be used to pass argument to your oauth_cb
        # It is not used in this example since we are passing all the arguments
        # from command line
        # 'sasl.oauthbearer.config': 'not-used',
        'oauth_cb': functools.partial(_get_token, args),
        'logger': logger,
        'on_delivery': delivery_report,
        'schema.registry.url': schema_registry_url
    }
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

args = dict()
args["username"] = username
args["password"] =  password
avroProducer = AvroProducer(producer_config(args=args), default_key_schema=key_schema, default_value_schema=value_schema)

data = pd.read_csv('traffic_data_small.csv')

i = 0
for index, row in data.iterrows():
  value = {"date": row["date"], "source": row["l_ipn"], "target": row["r_asn"],
                             "nflows": row["f"]}
  key = {"name": str(i)}
  i += 1
  avroProducer.produce(topic=topic_name, value=value, key=key)
  avroProducer.flush()