"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""

        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas

        self.topic_name = "org.chicago.cta.turnstile.events"
        super().__init__(
            self.topic_name,
            num_partitions=2,
            num_replicas=1,
        )

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated

        # logger.info(f"Sending turnstile event for station {self.station.station_id}")

        for _ in range(num_entries):
            timestamp = self.time_millis()
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": timestamp},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.station_name,
                    "line": self.station.color,
                },
                key_schema=Turnstile.key_schema,
                value_schema=Turnstile.value_schema,
            )
