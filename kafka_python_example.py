import threading
from kafka import KafkaProducer, KafkaConsumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import ssl
from kafka.sasl.oauth import AbstractTokenProvider

BOOTSTRAP_SERVERS="b-1.lrlrsitmsk.3zftdy.c2.kafka.us-east-2.amazonaws.com:9098"
TOPIC="assay-portal-data-change"
REGION="us-east-2"
ARN_ROLE="arn:aws:iam::014508419436:role/AssayPortal-MSK"

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        oauth2_token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(REGION, ARN_ROLE)
        return oauth2_token
    
def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_role_arn(REGION, ARN_ROLE)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

tp = MSKTokenProvider()

def start_producer():
    print("Starting producer")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
    )
    while True:
        msg = input("Producer > ")
        producer.send(TOPIC, msg.encode())
        producer.flush()

def start_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="test-group",
        auto_offset_reset="earliest",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
    )
    for msg in consumer:
        print("Consumer <", msg.value.decode())

if __name__ == "__main__":
    threading.Thread(target=start_producer).start()
    threading.Thread(target=start_consumer).start()
