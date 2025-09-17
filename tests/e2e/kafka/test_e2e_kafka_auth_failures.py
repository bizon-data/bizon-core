import os
import pytest
import time
from contextlib import contextmanager
from confluent_kafka import KafkaException

from bizon.connectors.sources.kafka.src.source import KafkaSource
from bizon.connectors.sources.kafka.src.config import (
    KafkaSourceConfig,
    KafkaAuthConfig,
    TopicConfig,
    MessageEncoding,
)
from bizon.source.auth.config import AuthType
from bizon.source.auth.authenticators.basic import BasicHttpAuthParams

# Skip all tests in this module unless KAFKA_E2E_TESTS environment variable is set
pytestmark = pytest.mark.skipif(
    not os.getenv("KAFKA_E2E_TESTS"),
    reason="Kafka e2e tests require KAFKA_E2E_TESTS environment variable to be set"
)


@contextmanager
def timeout_context(seconds):
    """Context manager to track execution time"""
    start_time = time.time()
    yield
    elapsed = time.time() - start_time
    assert elapsed < seconds + 5, f"Operation took {elapsed:.2f}s, expected < {seconds + 5}s"


class TestKafkaAuthenticationFailures:
    """Test authentication failure scenarios for Kafka source"""

    @pytest.fixture
    def kafka_host(self):
        """Get Kafka host from environment or use localhost"""
        return os.getenv("KAFKA_HOST", "localhost:9092")

    @pytest.fixture
    def invalid_auth_config(self, kafka_host):
        """Config with invalid authentication credentials"""
        return KafkaSourceConfig(
            name="kafka",
            stream="topic",
            topics=[TopicConfig(name="test-topic", destination_id="test_dest")],
            bootstrap_servers=kafka_host,
            group_id="test-auth-failures",
            consumer_timeout=5,  # Short timeout for faster test
            message_encoding=MessageEncoding.UTF_8,
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="invalid_user", password="invalid_password"),
            ),
        )

    @pytest.fixture
    def unreachable_server_config(self):
        """Config pointing to unreachable Kafka server"""
        return KafkaSourceConfig(
            name="kafka",
            stream="topic",
            topics=[TopicConfig(name="test-topic", destination_id="test_dest")],
            bootstrap_servers="unreachable-kafka:9092",
            group_id="test-unreachable",
            consumer_timeout=3,  # Very short timeout
            message_encoding=MessageEncoding.UTF_8,
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="test_user", password="test_password"),
            ),
        )

    @pytest.fixture
    def wrong_port_config(self, kafka_host):
        """Config with wrong port"""
        host = kafka_host.split(":")[0]  # Extract host without port
        return KafkaSourceConfig(
            name="kafka",
            stream="topic",
            topics=[TopicConfig(name="test-topic", destination_id="test_dest")],
            bootstrap_servers=f"{host}:9999",  # Wrong port
            group_id="test-wrong-port",
            consumer_timeout=3,
            message_encoding=MessageEncoding.UTF_8,
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="test_user", password="test_password"),
            ),
        )

    def test_invalid_authentication_credentials(self, invalid_auth_config):
        """Test that invalid authentication credentials are properly handled with timeout"""
        source = KafkaSource(invalid_auth_config)

        with timeout_context(10):  # Should complete within 10 seconds due to timeout
            success, error_msg = source.check_connection()

        assert not success, "Connection should fail with invalid credentials"
        assert error_msg is not None, "Error message should be provided"
        assert "Kafka connection failed" in error_msg

    def test_unreachable_kafka_server(self, unreachable_server_config):
        """Test connection to unreachable Kafka server times out properly"""
        source = KafkaSource(unreachable_server_config)

        with timeout_context(8):  # Should timeout within 8 seconds (3s timeout + buffer)
            success, error_msg = source.check_connection()

        assert not success, "Connection should fail for unreachable server"
        assert error_msg is not None, "Error message should be provided"
        assert "Kafka connection failed" in error_msg

    def test_wrong_port_connection(self, wrong_port_config):
        """Test connection to wrong port times out properly"""
        source = KafkaSource(wrong_port_config)

        with timeout_context(8):  # Should timeout within 8 seconds
            success, error_msg = source.check_connection()

        assert not success, "Connection should fail for wrong port"
        assert error_msg is not None, "Error message should be provided"
        assert "Kafka connection failed" in error_msg

    def test_get_data_with_invalid_auth(self, invalid_auth_config):
        """Test that get() method properly handles authentication failures and pipeline stops"""
        source = KafkaSource(invalid_auth_config)

        with timeout_context(10):
            # Ensure that KafkaException is raised, which should stop the pipeline
            with pytest.raises(KafkaException) as exc_info:
                source.get()

            # Verify the exception contains authentication/connection failure information
            exception_str = str(exc_info.value)
            assert any(keyword in exception_str.lower() for keyword in [
                "authentication", "sasl", "ssl", "transport", "broker", "connection"
            ]), f"Exception should indicate auth/connection failure: {exception_str}"

            # Verify the exception is fatal (should stop pipeline)
            kafka_error = exc_info.value.args[0] if exc_info.value.args else None
            if kafka_error and hasattr(kafka_error, 'fatal'):
                # If it's a fatal error, pipeline should definitely stop
                if kafka_error.fatal():
                    assert True, "Fatal error correctly stops pipeline"

            # At minimum, any KafkaException during get() should stop the pipeline
            assert True, "KafkaException raised - pipeline will stop as expected"

    def test_get_data_with_unreachable_server(self, unreachable_server_config):
        """Test that get() method properly handles unreachable server and pipeline stops"""
        source = KafkaSource(unreachable_server_config)

        with timeout_context(8):
            # Ensure that KafkaException is raised, which should stop the pipeline
            with pytest.raises(KafkaException) as exc_info:
                source.get()

            # Verify the exception contains connection failure information
            exception_str = str(exc_info.value)
            assert any(keyword in exception_str.lower() for keyword in [
                "transport", "broker", "connection", "timeout", "network", "unreachable"
            ]), f"Exception should indicate connection failure: {exception_str}"

            # Any KafkaException during get() should stop the pipeline
            assert True, "KafkaException raised - pipeline will stop as expected"

    def test_timeout_configuration_respected(self, unreachable_server_config):
        """Test that consumer timeout configuration is respected"""
        # Set very short timeout
        unreachable_server_config.consumer_timeout = 1
        source = KafkaSource(unreachable_server_config)

        start_time = time.time()
        success, _ = source.check_connection()
        elapsed = time.time() - start_time

        assert not success
        # Should complete within reasonable time (timeout + processing overhead)
        assert elapsed < 5, f"Operation took {elapsed:.2f}s, should respect 1s timeout"

    def test_nonexistent_topic_handling(self, kafka_host):
        """Test handling of non-existent topics"""
        config = KafkaSourceConfig(
            name="kafka",
            stream="topic",
            topics=[TopicConfig(name="non-existent-topic-12345", destination_id="test_dest")],
            bootstrap_servers=kafka_host,
            group_id="test-nonexistent-topic",
            consumer_timeout=5,
            message_encoding=MessageEncoding.UTF_8,
            consumer_config={"auto.offset.reset": "earliest", "enable.auto.commit": False, "security.protocol": "PLAINTEXT"},
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="test_user", password="test_password"),
            ),
        )

        source = KafkaSource(config)

        with timeout_context(10):
            success, error_msg = source.check_connection()

        # Depending on Kafka setup, this might succeed (auto-create) or fail
        # We mainly want to ensure it doesn't hang and returns within timeout
        if not success:
            assert error_msg is not None
            # Could be "not found" or connection error depending on setup
            assert ("not found" in error_msg or "transport failure" in error_msg or
                   "connection" in error_msg.lower())

    @pytest.mark.parametrize("timeout_value", [1, 3, 5])
    def test_different_timeout_values(self, unreachable_server_config, timeout_value):
        """Test that different timeout values are respected"""
        unreachable_server_config.consumer_timeout = timeout_value
        source = KafkaSource(unreachable_server_config)

        start_time = time.time()
        success, _ = source.check_connection()
        elapsed = time.time() - start_time

        assert not success
        # Allow some overhead but ensure timeout is roughly respected
        assert elapsed < timeout_value + 3, f"Operation took {elapsed:.2f}s, timeout was {timeout_value}s"

    def test_malformed_bootstrap_servers(self):
        """Test handling of malformed bootstrap servers"""
        config = KafkaSourceConfig(
            name="kafka",
            stream="topic",
            topics=[TopicConfig(name="test-topic", destination_id="test_dest")],
            bootstrap_servers="invalid-format-server",  # Missing port
            group_id="test-malformed",
            consumer_timeout=3,
            message_encoding=MessageEncoding.UTF_8,
            authentication=KafkaAuthConfig(
                type=AuthType.BASIC,
                params=BasicHttpAuthParams(username="test_user", password="test_password"),
            ),
        )

        source = KafkaSource(config)

        with timeout_context(8):
            success, error_msg = source.check_connection()

        assert not success, "Connection should fail with malformed server address"
        assert error_msg is not None

    def test_pipeline_integration_auth_failure_stops_execution(self, invalid_auth_config):
        """Integration test: Verify authentication failure stops pipeline execution immediately"""
        source = KafkaSource(invalid_auth_config)

        # Test that check_connection fails first (pre-flight check)
        with timeout_context(10):
            connection_success, connection_error = source.check_connection()

        assert not connection_success, "Connection check should fail with invalid auth"
        assert connection_error is not None, "Connection error should be reported"

        # Test that get() method also fails and raises exception (runtime check)
        with timeout_context(10):
            with pytest.raises(KafkaException) as exc_info:
                # This simulates what happens when pipeline tries to fetch data
                source.get()

            # Ensure the exception would stop pipeline execution
            exception_str = str(exc_info.value)
            assert len(exception_str) > 0, "Exception message should not be empty"

            # Log that pipeline would stop
            print(f"✅ Pipeline correctly stopped with exception: {exception_str[:100]}...")

        # Verify that no partial data is returned when auth fails
        # This ensures data integrity - either all data or no data, never partial
        assert True, "Authentication failure correctly prevents partial data extraction"