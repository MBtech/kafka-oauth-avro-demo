from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import logging 
import functools
from keycloak import KeycloakOpenID
import time
from config import *

topic_name = "traffic-avro"

oidc_obj = KeycloakOpenID(server_url=keycloak_url,
                    client_id=client_id,
                    realm_name=realm_name,
                    client_secret_key=client_secret_key)


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


def consumer_config(args):
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
        'group.id': 'test1',
        'schema.registry.url': schema_registry_url
    }


args = dict()
args["username"] = username
args["password"] =  password
c = AvroConsumer(consumer_config(args))

c.subscribe([topic_name])

while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()