from bizon.connectors.sources.kafka.src.source import KafkaAuthConfig, KafkaSourceConfig


def test_kafka_source_config():
    conf = KafkaSourceConfig(
        name="kafka",
        stream="topic",
        topic="cookie",
        bootstrap_servers="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )

    assert conf.nb_bytes_schema_id == 4


def test_kafka_source_config_timestamp_to_parse():
    conf = KafkaSourceConfig(
        name="kafka",
        stream="topic",
        topic="cookie",
        bootstrap_servers="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )
    assert conf
