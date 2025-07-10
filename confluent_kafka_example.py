from confluent_kafka import Producer, Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import threading
import socket
import ssl
BOOTSTRAP_SERVERS="<server::localhost>:<port::9098>"
TOPIC="<topic-name::test-topic>"
REGION="<region::us-east-2>"
ARN_ROLE="arn:aws:iam::<account::xxxxxxxx>:role/<role-name::Test-MSK>"

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_role_arn(REGION, ARN_ROLE)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

conf = {
    "debug": "all",
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname(),
    'oauth_cb': oauth_cb,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    # "sasl.oauthbearer.config": f"token={token}",
    "group.id": "test-group",
    "auto.offset.reset": "earliest"
}

def start_producer():
    p = Producer(conf)
    while True:
        msg = input("Producer > ")
        p.produce(TOPIC, msg.encode())
        p.flush()

def start_consumer():
    c = Consumer(conf)
    c.subscribe([TOPIC])
    while True:
        msg = c.poll(1.0)
        if msg and not msg.error():
            print("Consumer <", msg.value().decode())

if __name__ == "__main__":
    threading.Thread(target=start_producer).start()
    threading.Thread(target=start_consumer).start()
