"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "BROKER_URL": "PLAINTEXT://localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        }
        self.client = AdminClient(
            {"bootstrap.servers": self.broker_properties.get("BROKER_URL")}
        )
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties.get("BROKER_URL"),
                "schema.registry.url": self.broker_properties.get(
                    "SCHEMA_REGISTRY_URL"
                ),
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = self.client
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for _, future in futures.items():
            try:
                future.result()
                logger.info("topics created")
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                logger.info("topic creation kafka integration incomplete - skipping")

    # def time_millis(self):
    #     return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # TODO: Write cleanup code for the Producer here
        fs = self.client.delete_topics(self.topic_name, operation_timeout=30)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print(f"failed to delete topic {self.topic_name}: {e}")
                logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
