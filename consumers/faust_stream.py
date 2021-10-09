"""Defines trends calculations for stations"""
import logging
from confluent_kafka.avro import serializer

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
src_topic_name = "org.chicago.cta.postgres.table.stations"
src_topic = app.topic(src_topic_name, value_type=Station)

# TODO: Define the output Kafka Topic
changelog_topic_name = "org.chicago.cta.stations.table.v1"
changelog_topic = app.topic(changelog_topic_name, partitions=1)

# TODO: Define a Faust Table
stations_table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=changelog_topic,
)


# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`


@app.agent(src_topic)
async def process(stream):
    async for station in stream:
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"

        transformed = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )

        stations_table[station.station_id] = transformed


if __name__ == "__main__":
    app.main()
