from bizon.connectors.sources.kafka.src.config import KafkaAuthConfig, TopicConfig
from bizon.connectors.sources.kafka.src.source import KafkaSource, KafkaSourceConfig


def test_kafka_source_config_timestamp_to_parse():
    conf = KafkaSourceConfig(
        name="kafka",
        stream="topic",
        topics=[
            TopicConfig(name="cookie", destination_id="cookie"),
        ],
        bootstrap_servers="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )
    assert conf


def test_kafka_source_config_check_connection():
    conf = KafkaSourceConfig(
        name="kafka",
        stream="topic",
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
        bootstrap_servers="fdjvfv",  # Invalid bootstrap server
        topics=[
            TopicConfig(name="cookie", destination_id="cookie"),
        ],
    )

    source = KafkaSource(config=conf)
    success, error = source.check_connection()

    # Should fail due to invalid bootstrap server and/or credentials
    assert success is False
    assert error is not None
    assert "connection" in error.lower() or "failed" in error.lower()