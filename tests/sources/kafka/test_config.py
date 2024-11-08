from bizon.sources.kafka.src.source import (
    KafkaAuthConfig,
    KafkaSource,
    KafkaSourceConfig,
)


def test_kafka_source_config():
    conf = KafkaSourceConfig(
        source_name="kafka",
        stream_name="topic",
        topic="cookie",
        bootstrap_server="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )

    assert conf.nb_bytes_schema_id == 4
    assert conf.timestamp_ms_name == ""


def test_kafka_source_config_timestamp_to_parse():
    conf = KafkaSourceConfig(
        source_name="kafka",
        stream_name="topic",
        topic="cookie",
        bootstrap_server="fdjvfv",
        timestamp_ms_name="timestamp",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )
    assert conf.timestamp_ms_name == "timestamp"


def test_kafka_source():
    conf = KafkaSourceConfig(
        source_name="kafka",
        stream_name="topic",
        topic="cookie",
        bootstrap_server="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )

    source = KafkaSource(conf)
    assert source.parse_timestamp is False
    del source


def test_kafka_source_parse_timestamp():
    conf = KafkaSourceConfig(
        source_name="kafka",
        stream_name="topic",
        topic="cookie",
        bootstrap_server="fdjvfv",
        timestamp_ms_name="timestamp",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )

    source = KafkaSource(conf)
    assert source.parse_timestamp is True
    del source
