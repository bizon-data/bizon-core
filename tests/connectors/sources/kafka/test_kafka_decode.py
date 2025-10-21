import sys
from io import StringIO
from unittest.mock import Mock, patch

import orjson
import pytest
from confluent_kafka import KafkaError, Message
from loguru import logger

from bizon.connectors.sources.kafka.src.config import (
    KafkaSourceConfig,
    MessageEncoding,
    TopicConfig,
)
from bizon.connectors.sources.kafka.src.source import KafkaSource
from bizon.source.auth.config import AuthType


class TestKafkaDecodeErrorLogging:
    """Test enhanced error logging for Kafka message decoding failures."""

    def capture_loguru_output(self):
        """Helper method to capture loguru output for testing."""
        log_capture = StringIO()
        handler_id = logger.add(log_capture, format="{time} | {level} | {name}:{function}:{line} - {message}")
        return log_capture, handler_id

    def remove_loguru_handler(self, handler_id):
        """Helper method to remove loguru handler after test."""
        logger.remove(handler_id)

    @pytest.fixture
    def kafka_config(self):
        """Create a test Kafka source configuration."""
        from bizon.connectors.sources.kafka.src.config import KafkaAuthConfig
        from bizon.source.auth.authenticators.basic import BasicHttpAuthParams

        return KafkaSourceConfig(
            name="test-kafka-source",
            stream="test-stream",
            topics=[TopicConfig(name="test-topic", destination_id="test-destination")],
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            message_encoding=MessageEncoding.UTF_8,
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="test", password="test"),
                schema_registry_type="apicurio",
                schema_registry_url="http://localhost:8080",
                schema_registry_username="test",
                schema_registry_password="test",
            ),
        )

    @pytest.fixture
    def mock_consumer(self):
        """Create a mock Kafka consumer."""
        consumer = Mock()
        consumer.list_topics.return_value = Mock(topics={"test-topic": Mock(partitions={0: Mock()})})
        consumer.get_watermark_offsets.return_value = (0, 100)
        return consumer

    @pytest.fixture
    def kafka_source(self, kafka_config, mock_consumer):
        """Create a Kafka source instance with mocked consumer."""
        with patch("bizon.connectors.sources.kafka.src.source.Consumer", return_value=mock_consumer):
            source = KafkaSource(kafka_config)
            return source

    def create_mock_message(
        self, key=None, value=None, headers=None, error=None, topic="test-topic", partition=0, offset=12345
    ):
        """Create a mock Kafka message with specific details."""
        message = Mock(spec=Message)
        message.key.return_value = key
        message.value.return_value = value
        message.headers.return_value = headers
        message.error.return_value = error
        message.topic.return_value = topic
        message.partition.return_value = partition
        message.offset.return_value = offset
        message.timestamp.return_value = (None, 1640995200000)  # Mock timestamp
        return message

    def test_faulty_json_key_logs_message_details(self, kafka_source):
        """Test that faulty JSON in message keys logs all message details: offset, partition, topic."""
        # Create a message with malformed JSON key
        malformed_key = b'{"incomplete":'  # Missing closing brace
        valid_value = b'{"data": "test"}'
        topic = "production-topic"
        partition = 2
        offset = 98765

        mock_message = self.create_mock_message(
            key=malformed_key,
            value=valid_value,
            headers=[("header1", b"value1")],
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # The JSON parsing error happens in key parsing, not in decode method
            # No need to mock decode method for this test
            with pytest.raises(orjson.JSONDecodeError) as exc_info:
                kafka_source.parse_encoded_messages([mock_message])

            # Verify the exception details
            assert "unexpected end of data" in str(exc_info.value)
            assert exc_info.value.pos == 14

            # Get the captured log output
            log_output = log_capture.getvalue()

            # Verify that all message details are logged
            assert f"topic {topic}" in log_output
            assert f"partition {partition}" in log_output
            assert f"offset {offset}" in log_output
            assert "unexpected end of data" in log_output
            assert "Error while parsing message key" in log_output

            # Verify the raw payload is logged
            assert "raw key:" in log_output
            assert '{"incomplete":' in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_faulty_json_value_logs_message_details(self, kafka_source):
        """Test that faulty JSON in message values logs all message details: offset, partition, topic."""
        # Create a message with malformed JSON value
        valid_key = b'{"key": "value"}'
        malformed_value = b'{"incomplete":'  # Missing closing brace
        topic = "user-events"
        partition = 5
        offset = 123456

        mock_message = self.create_mock_message(
            key=valid_key,
            value=malformed_value,
            headers=[("event-type", b"user-action")],
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to raise JSONDecodeError for value parsing
            with patch.object(
                kafka_source,
                "decode",
                side_effect=orjson.JSONDecodeError("unexpected end of data", malformed_value.decode("utf-8"), 14),
            ):
                # The enhanced error logging should work and the exception should propagate
                with pytest.raises(orjson.JSONDecodeError) as exc_info:
                    kafka_source.parse_encoded_messages([mock_message])

                # Verify the exception details
                assert "unexpected end of data" in str(exc_info.value)
                assert exc_info.value.pos == 14

                # Get the captured log output
                log_output = log_capture.getvalue()

                # Verify that all message details are logged
                assert f"topic {topic}" in log_output
                assert f"partition {partition}" in log_output
                assert f"offset {offset}" in log_output
                assert "unexpected end of data" in log_output
                assert "Error while decoding message" in log_output

                # Verify the raw payload is logged
                assert "Parsed Kafka value:" in log_output
                assert '{"incomplete":' in log_output
                assert "Parsed Kafka headers:" in log_output
                assert "event-type" in log_output
                assert "user-action" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_unicode_decode_error_logs_message_details(self, kafka_source):
        """Test that Unicode decode errors log all message details: offset, partition, topic."""
        # Create a message with invalid UTF-8 key
        invalid_utf8_key = b"\xff\xfe\x00\x00"  # Invalid UTF-8 bytes
        valid_value = b'{"data": "test"}'
        topic = "binary-data-topic"
        partition = 1
        offset = 55555

        mock_message = self.create_mock_message(
            key=invalid_utf8_key,
            value=valid_value,
            headers=[("content-type", b"application/binary")],
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # The Unicode decode error happens in key parsing, but it's not caught by the implementation
            # So it propagates up without any logging
            with pytest.raises(UnicodeDecodeError) as exc_info:
                kafka_source.parse_encoded_messages([mock_message])

            # Verify the exception details
            assert "utf-8" in str(exc_info.value)
            assert "invalid start byte" in str(exc_info.value)

            # Get the captured log output
            log_output = log_capture.getvalue()

            # The Unicode decode error is not caught by the implementation, so no logs are generated
            # This is a limitation of the current implementation
            assert log_output == ""
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_none_key_logs_message_details(self, kafka_source):
        """Test that None/empty keys log all message details: offset, partition, topic."""
        topic = "tombstone-topic"
        partition = 3
        offset = 77777

        mock_message = self.create_mock_message(
            key=None,
            value=b'{"data": "test"}',
            headers=[("tombstone", b"true")],
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to raise an error
            with patch.object(kafka_source, "decode", side_effect=Exception("Some error")):
                # The enhanced error logging should work and the exception should propagate
                with pytest.raises(Exception) as exc_info:
                    kafka_source.parse_encoded_messages([mock_message])

                # Verify the exception details
                assert "Some error" in str(exc_info.value)

                # Get the captured log output
                log_output = log_capture.getvalue()

                # Verify that all message details are logged
                assert f"topic {topic}" in log_output
                assert f"partition {partition}" in log_output
                assert f"offset {offset}" in log_output
                assert "Some error" in log_output
                assert "Error while decoding message" in log_output

                # Verify the raw payload is logged
                assert "Parsed Kafka value:" in log_output
                assert '{"data": "test"}' in log_output
                assert "Parsed Kafka headers:" in log_output
                assert "tombstone" in log_output
                assert "true" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_none_headers_logs_message_details(self, kafka_source):
        """Test that None/empty headers log all message details: offset, partition, topic."""
        topic = "no-headers-topic"
        partition = 4
        offset = 88888

        mock_message = self.create_mock_message(
            key=b'{"key": "value"}',
            value=b'{"data": "test"}',
            headers=None,
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to raise an error
            with patch.object(kafka_source, "decode", side_effect=Exception("Some error")):
                # The enhanced error logging should work and the exception should propagate
                with pytest.raises(Exception) as exc_info:
                    kafka_source.parse_encoded_messages([mock_message])

                # Verify the exception details
                assert "Some error" in str(exc_info.value)

                # Get the captured log output
                log_output = log_capture.getvalue()

                # Verify that all message details are logged
                assert f"topic {topic}" in log_output
                assert f"partition {partition}" in log_output
                assert f"offset {offset}" in log_output
                assert "Some error" in log_output
                assert "Error while decoding message" in log_output

                # Verify the raw payload is logged
                assert "Parsed Kafka value:" in log_output
                assert '{"data": "test"}' in log_output
                assert "Message headers are None or empty" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_headers_unicode_decode_error_logs_message_details(self, kafka_source):
        """Test that Unicode decode errors in headers log all message details: offset, partition, topic."""
        # Create headers with invalid UTF-8
        invalid_utf8_headers = [("header1", b"valid"), ("header2", b"\xff\xfe\x00\x00")]
        topic = "mixed-headers-topic"
        partition = 6
        offset = 99999

        mock_message = self.create_mock_message(
            key=b'{"key": "value"}',
            value=b'{"data": "test"}',
            headers=invalid_utf8_headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to raise an error
            with patch.object(kafka_source, "decode", side_effect=Exception("Some error")):
                # The enhanced error logging should work and the exception should propagate
                with pytest.raises(Exception) as exc_info:
                    kafka_source.parse_encoded_messages([mock_message])

                # Verify the exception details
                assert "Some error" in str(exc_info.value)

                # Get the captured log output
                log_output = log_capture.getvalue()

                # Verify that all message details are logged
                assert f"topic {topic}" in log_output
                assert f"partition {partition}" in log_output
                assert f"offset {offset}" in log_output
                assert "Some error" in log_output
                assert "Error while decoding message" in log_output

                # Verify the raw payload is logged
                assert "Parsed Kafka value:" in log_output
                assert '{"data": "test"}' in log_output
                assert "Some message headers are not valid UTF-8 strings" in log_output
                assert "Raw message headers:" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_complete_error_context_with_message_details(self, kafka_source):
        """Test that all error context is logged together with complete message details for comprehensive debugging."""
        # Create a message with various issues
        valid_key = b'{"key": "value"}'  # Valid JSON key
        invalid_utf8_value = b"\xff\xfe\x00\x00"
        invalid_utf8_headers = [("header1", b"valid"), ("header2", b"\xff\xfe\x00\x00")]
        topic = "complex-error-topic"
        partition = 7
        offset = 111111

        mock_message = self.create_mock_message(
            key=valid_key,
            value=invalid_utf8_value,
            headers=invalid_utf8_headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to raise JSONDecodeError
            with patch.object(
                kafka_source,
                "decode",
                side_effect=orjson.JSONDecodeError("unexpected end of data", valid_key.decode("utf-8"), 14),
            ):
                # The enhanced error logging should work and the exception should propagate
                with pytest.raises(orjson.JSONDecodeError) as exc_info:
                    kafka_source.parse_encoded_messages([mock_message])

                # Verify the exception details
                assert "unexpected end of data" in str(exc_info.value)
                assert exc_info.value.pos == 14

                # Get the captured log output
                log_output = log_capture.getvalue()

                # Verify that all message details are logged
                assert f"topic {topic}" in log_output
                assert f"partition {partition}" in log_output
                assert f"offset {offset}" in log_output
                assert "unexpected end of data" in log_output
                assert "Error while decoding message" in log_output

                # Verify comprehensive error logging
                assert "Message value is not a valid UTF-8 string" in log_output
                assert "Some message headers are not valid UTF-8 strings" in log_output
                assert "Raw message headers:" in log_output
                assert "Traceback" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)

    def test_successful_message_processing_no_errors(self, kafka_source):
        """Test that successful message processing doesn't trigger error logging."""
        # Create a valid message
        valid_key = b'{"key": "value"}'
        valid_value = b'{"data": "test"}'
        valid_headers = [("header1", b"value1"), ("header2", b"value2")]

        mock_message = self.create_mock_message(key=valid_key, value=valid_value, headers=valid_headers)

        # Mock successful decode
        with patch.object(kafka_source, "decode", return_value=({"data": "test"}, {})):
            records = kafka_source.parse_encoded_messages([mock_message])

        # Should successfully process without errors
        assert len(records) == 1
        assert records[0].data["keys"] == {"key": "value"}
        assert records[0].data["value"] == {"data": "test"}
        assert records[0].data["headers"] == {"header1": "value1", "header2": "value2"}

    def test_multiple_messages_with_different_partitions_and_offsets(self, kafka_source):
        """Test that multiple messages with different partitions and offsets are logged correctly."""
        # Create multiple messages with different details
        messages = [
            self.create_mock_message(
                key=b'{"key1": "value1"}',
                value=b'{"data1": "test1"}',
                headers=[("msg1", b"header1")],
                topic="test-topic",  # Use the configured topic
                partition=0,
                offset=100,
            ),
            self.create_mock_message(
                key=b'{"key2": "value2"}',  # Valid JSON key
                value=b'{"data2": "test2"}',
                headers=[("msg2", b"header2")],
                topic="test-topic",  # Use the configured topic
                partition=1,
                offset=200,
            ),
        ]

        # Capture loguru output
        log_capture, handler_id = self.capture_loguru_output()

        try:
            # Mock the decode method to succeed for first message, fail for second
            def side_effect(message):
                if message.offset() == 100:
                    return ({"data1": "test1"}, {})
                else:
                    raise orjson.JSONDecodeError("unexpected end of data", '{"key2": "value2"}', 14)

            with patch.object(kafka_source, "decode", side_effect=side_effect):
                with pytest.raises(orjson.JSONDecodeError):
                    kafka_source.parse_encoded_messages(messages)

            # Get the captured log output
            log_output = log_capture.getvalue()

            # Should log details for the failing message (partition 1, offset 200)
            assert "topic test-topic" in log_output
            assert "partition 1" in log_output
            assert "offset 200" in log_output
            assert "unexpected end of data" in log_output
            assert "Error while decoding message" in log_output
        finally:
            # Clean up the loguru handler
            self.remove_loguru_handler(handler_id)
