import asyncio
from aiokafka import AIOKafkaProducer as KafkaProducer, AIOKafkaConsumer as KafkaConsumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import ssl
from aiokafka.abc import AbstractTokenProvider
import socket
import json

BOOTSTRAP_SERVERS="<server::localhost>:<port::9098>"
TOPIC="<topic-name::test-topic>"
REGION="<region::us-east-2>"
ARN_ROLE="arn:aws:iam::<account::xxxxxxxx>:role/<role-name::Test-MSK>"

def setup_ssl_context():
    """Set up SSL context for Kafka connection."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_OPTIONAL
    ssl_context.load_default_certs()
    return ssl_context

class CustomTokenProvider(AbstractTokenProvider):
    async def token(self):
        return await asyncio.get_running_loop().run_in_executor(None, self._token)

    def _token(self):
        oauth2_token, _ = MSKAuthTokenProvider.generate_auth_token_from_role_arn(REGION, ARN_ROLE)
        return oauth2_token
       


async def produce():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=socket.gethostname(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        ssl_context=setup_ssl_context(),
        sasl_oauth_token_provider=CustomTokenProvider(),
    )
    await producer.start()
    try:
        while True:
            msg = input("Producer > ")
            await producer.send(TOPIC, msg)
            await producer.flush()

    finally:
        await producer.stop()

async def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="test-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        ssl_context=setup_ssl_context(),
        sasl_oauth_token_provider=CustomTokenProvider(),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print("Consumer <", msg.value)
    finally:
        await consumer.stop()

async def main():
    await asyncio.gather(produce(), consume())

if __name__ == "__main__":
    asyncio.run(main())
