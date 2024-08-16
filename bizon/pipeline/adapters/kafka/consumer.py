from threading import Thread

from kafka import KafkaConsumer
from loguru import logger

from bizon.common.config import KafkaConsumerConfig
from bizon.pipeline.consumer import AbstractConsumer


class ThreadedKafkaConsumer(Thread, AbstractConsumer):
    def __init__(self, config: KafkaConsumerConfig, topic: str):
        Thread.__init__(self)
        AbstractConsumer.__init__(self, config=config)
        self.config: KafkaConsumerConfig = config
        self.consumer = self.get_consumer()
        self.topic = topic
        self.consumer.subscribe(topic)

    def get_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=self.config.enable_auto_commit,
            consumer_timeout_ms=self.config.consumer_timeout_ms,
        )

    def run(self):
        try:
            for message in self.consumer:
                topic_info = f"topic: {message.partition}|{message.offset})"
                message_info = f"key: {message.key}, {message.value}"
                logger.info(f"{topic_info}, {message_info}")
        except Exception as e:
            logger.error(f"Error occurred while consuming messages: {e}")
        finally:
            self.consumer.close()
