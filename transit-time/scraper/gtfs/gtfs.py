"""
Parse a GTFS feed.  In an ideal world, this would work with lots of feeds,
but right now a lot of it is MTA specific and will need to be modified if
we want to support other feeds.
"""

import enum
from datetime import date, datetime, timezone
from typing import Dict, List, NamedTuple, Optional

from scraper.gtfs import gtfs_realtime_pb2, nyct_subway_pb2


@enum.unique
class TransitSystem(enum.Enum):
    NYC_MTA = "nyc_mta"


def get_system_name(system: TransitSystem) -> str:
    if system == TransitSystem.NYC_MTA:
        return "NYCT Subway"
    else:
        raise Exception("Unhandled TransitSystem: {}".format(system))


@enum.unique
class LocationType(enum.Enum):
    STOP = 0
    STATION = 1
    ENTRANCE_EXIT = 2
    GENERIC_NODE = 3
    BOARDING_AREA = 4


@enum.unique
class VehicleStopStatus(enum.Enum):
    INCOMING_AT = enum.auto()
    STOPPED_AT = enum.auto()
    IN_TRANSIT_TO = enum.auto()


class TripDescriptor(NamedTuple):
    trip_id: str
    route_id: Optional[str]
    start_date: date


class StopTimeUpdate(NamedTuple):
    stop_id: str
    arrival: Optional[datetime]
    departure: Optional[datetime]


class TripUpdate(NamedTuple):
    trip: TripDescriptor
    stop_time_updates: List[StopTimeUpdate]
    timestamp: Optional[datetime]


class VehiclePosition(NamedTuple):
    trip: TripDescriptor
    current_stop_sequence: Optional[int]
    stop_id: str
    current_status: VehicleStopStatus
    timestamp: Optional[datetime]


class FeedMessage:
    system: TransitSystem
    feed_id: str
    timestamp: datetime
    # MTA extension.  route_id -> end_time
    trip_replacements: Dict[str, datetime]
    trip_updates: List[TripUpdate]
    vehicle_positions: List[VehiclePosition]

    def __init__(self, system: TransitSystem, feed_id: str, timestamp: datetime):
        self.system = system
        self.feed_id = feed_id
        self.timestamp = timestamp
        self.trip_replacements = {}
        self.trip_updates = []
        self.vehicle_positions = []

    def is_trip_replaced(self, route_id: str) -> bool:
        if route_id not in self.trip_replacements:
            return False
        return self.trip_replacements[route_id] >= self.timestamp


def _parse_trip_descriptor(trip_pb, transit_system: TransitSystem) -> TripDescriptor:
    # Date is formatted as YYYYMMDD
    assert len(trip_pb.start_date) == 8
    start_date = date(
        int(trip_pb.start_date[0:4]),
        int(trip_pb.start_date[4:6]),
        int(trip_pb.start_date[6:8]),
    )

    # Sometimes the route_id is an empty string, who knows why?
    if trip_pb.HasField("route_id") and trip_pb.route_id != "":
        route_id = trip_pb.route_id
    else:
        route_id = None

    # The MTA sends an 'SS' route which is not in routes.txt.  It looks like
    # these are Staten Island Railroad routes
    # https://groups.google.com/forum/#!searchin/mtadeveloperresources/ss|sort:date/mtadeveloperresources/_YtdbNl1FXo/0rNUIkjfBgAJ
    if transit_system == TransitSystem.NYC_MTA and route_id == "SS":
        route_id = "SI"

    return TripDescriptor(
        trip_id=trip_pb.trip_id, route_id=route_id, start_date=start_date
    )


def _parse_stop_time_update(stop_time_update_pb) -> StopTimeUpdate:
    arrival = None
    if stop_time_update_pb.HasField("arrival"):
        arrival = datetime.fromtimestamp(stop_time_update_pb.arrival.time, timezone.utc)

    departure = None
    if stop_time_update_pb.HasField("departure"):
        departure = datetime.fromtimestamp(
            stop_time_update_pb.departure.time, timezone.utc
        )

    return StopTimeUpdate(
        stop_id=stop_time_update_pb.stop_id, arrival=arrival, departure=departure
    )


def _parse_trip_update(trip_update_pb, transit_system: TransitSystem) -> TripUpdate:
    timestamp = None
    if trip_update_pb.HasField("timestamp"):
        timestamp = datetime.fromtimestamp(trip_update_pb.timestamp, timezone.utc)

    stop_time_updates = []
    for stop_time_update in trip_update_pb.stop_time_update:
        stop_time_updates.append(_parse_stop_time_update(stop_time_update))

    return TripUpdate(
        trip=_parse_trip_descriptor(trip_update_pb.trip, transit_system),
        stop_time_updates=stop_time_updates,
        timestamp=timestamp,
    )


def _parse_vehicle_position(
    vehicle_pb, transit_system: TransitSystem
) -> VehiclePosition:
    current_status = VehicleStopStatus.IN_TRANSIT_TO
    if vehicle_pb.HasField("current_status"):
        stop_status_enum = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus
        if vehicle_pb.current_status == stop_status_enum.INCOMING_AT:
            current_status = VehicleStopStatus.INCOMING_AT
        elif vehicle_pb.current_status == stop_status_enum.STOPPED_AT:
            current_status = VehicleStopStatus.STOPPED_AT
        elif vehicle_pb.current_status == stop_status_enum.IN_TRANSIT_TO:
            current_status = VehicleStopStatus.IN_TRANSIT_TO
        else:
            raise ValueError(
                "Unknown VehicleStopStatus: {}".format(vehicle_pb.current_status)
            )

    return VehiclePosition(
        trip=_parse_trip_descriptor(vehicle_pb.trip, transit_system),
        current_stop_sequence=(
            vehicle_pb.current_stop_sequence
            if vehicle_pb.HasField("current_stop_sequence")
            else None
        ),
        stop_id=vehicle_pb.stop_id if vehicle_pb.HasField("stop_id") else None,
        current_status=current_status,
        timestamp=(
            datetime.fromtimestamp(vehicle_pb.timestamp, timezone.utc)
            if vehicle_pb.HasField("timestamp")
            else None
        ),
    )


def parse_feed_message(
    transit_system: TransitSystem, feed_id: str, feed_message_pb
) -> FeedMessage:
    header = feed_message_pb.header
    timestamp = datetime.fromtimestamp(header.timestamp, timezone.utc)
    feed_message = FeedMessage(transit_system, feed_id, timestamp,)

    if header.HasExtension(nyct_subway_pb2.nyct_feed_header):
        nyct_header = header.Extensions[nyct_subway_pb2.nyct_feed_header]
        for replacement in nyct_header.trip_replacement_period:
            route_id = replacement.route_id
            end_time = datetime.fromtimestamp(
                replacement.replacement_period.end, timezone.utc
            )
            feed_message.trip_replacements[route_id] = end_time
            # MTA data has a bug where they use route_id 'S' instead of 'GS'
            # in feed 1.  ('S' is not a valid route_id)
            if feed_message.feed_id == "1" and route_id == "S":
                # Don't replace 'GS' if MTA fixed their data
                if "GS" not in feed_message.trip_replacements:
                    feed_message.trip_replacements["GS"] = end_time
            # The 6X is probably replaced when the 7 is replaced
            if route_id == "6":
                if "6X" not in feed_message.trip_replacements:
                    feed_message.trip_replacements["6X"] = end_time
            # The 7X is probably replaced when the 7 is replaced
            if route_id == "7":
                if "7X" not in feed_message.trip_replacements:
                    feed_message.trip_replacements["7X"] = end_time

    for entity in feed_message_pb.entity:
        if entity.HasField("trip_update"):
            feed_message.trip_updates.append(
                _parse_trip_update(entity.trip_update, transit_system)
            )
        if entity.HasField("vehicle"):
            feed_message.vehicle_positions.append(
                _parse_vehicle_position(entity.vehicle, transit_system)
            )
        # Implement Alert import here if necessary

    return feed_message
