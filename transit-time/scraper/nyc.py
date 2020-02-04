"""
Handler for NYC Subway data.

GTFS Feeds: https://datamine.mta.info/list-of-feeds
GTFS info: https://datamine.mta.info/feed-documentation
Files: http://web.mta.info/developers/developer-data-terms.html
"""

import enum
import logging
import os
import re
from typing import List, Optional, NamedTuple

from scraper import networking

from scraper.gtfs import gtfs_realtime_pb2, nyct_subway_pb2  # noqa: F401


# https://datamine.mta.info/list-of-feeds
FEED_URL = "http://datamine.mta.info/mta_esi.php"
FEED_IDS: List[str] = [
    # 1 2 3, 4 5 6, S (42nd St)
    "1",
    # A C E H (??), S (Franklin Ave)
    "26",
    # N Q R W
    "16",
    # B D F M
    "21",
    # L
    "2",
    # Staten Island Railway
    "11",
    # G
    "31",
    # J Z
    "36",
    # 7
    "51",
]


# The API key is stored in production using Kubernetes Secrets.  To use an
# API key, run the following command (replacing {{API_KEY}} with your key):
#   $ kubectl create secret generic api-keys --from-literal=nyc-mta={{API_KEY}}
# For local testing, we can just set th environment variable.
def __get_api_key():
    return os.environ["NYC_MTA_API_KEY"]


async def get_data(feed_id: str):
    logging.info("Downloading NYC subway data for feed %s", feed_id)
    params = {
        "key": __get_api_key(),
        "feed_id": feed_id,
    }
    content = await networking.get_content(FEED_URL, params)

    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.ParseFromString(content)
    return feed_message


# Given a full trip ID from trips.txt, break it down into parts.
# This is described in the MTA GTFS specification.
# Origin time, Route ID, and Direction can be used to identify a unique trip.
# (NOTE: actually it's only unique for each service code, there are trips
# with the same origin/route/destination on Saturday and Sunday)
# Path identifier is optional data that is only provided when known.
#
# Example: A20111204SAT_021150_2..N08R
# - A: sub-division identifier, either A (IRT) or B (BMT and IND)
# - 20110204: effective date of Dec 4, 2011
# - SAT: applicable service code, typically WKD (weekday), SAT (Saturday), or
#   SUN (Sunday)
# - 021150: trip origin time, in hundredths of a minute past midnight
# - 2..N08R: Trip path.  Decomposes further into
#   - 2: route id (2 train)
#   - N: direction (northbound)
#   - 08R: path identifier
#
# HA HA JUST JOKING!!
#
# The MTA provides the above example, but the real data is nothing like that.
# Instead, ids look like this:
#   AFA19GEN-1037-Sunday-00_010600_1..S03R
#   AFA19GEN-GS010-Saturday-00_036400_GS.S01R
#   BFA19GEN-N058-Sunday-00_096300_N..S36R
#   BFA19SUPP-L024-Sunday-99_090000_L..N01R
#   SIR-FA2017-SI017-Weekday-08_121100_SI..N03R
# Here's my guess what everything is.  We test the parser by making sure
# the entirety of trips.txt parses.
# - A: sub-division identifier
# - FA19GEN: effective date Fall 2019?
# - 1: route id
# - 037: ???
# - Sunday: applicable service code
# - 00: ???
# - 010600: trip origin time, in hundredths of a minute past midnight
# - 1..S03R: Trip path.
#   - 1: route id (1 train).  Suffix is one or two dots
#   - S: direction (southbound)
#   - 03R: path identifier


@enum.unique
class ServiceDay(enum.Enum):
    WEEKDAY = "Weekday"
    SATURDAY = "Saturday"
    SUNDAY = "Sunday"


class TripID(NamedTuple):
    sub_division: str
    effective_date: str
    service_day: ServiceDay
    origin_time: str
    trip_path: str
    route_id: str
    direction: str
    path_identifier: str


# Since we don't have a spec for trip ID, make this extra strict and throw
# if we encounter an unexpected trip ID format.
TRIP_ID_RE = r"""
    (?P<sub_division>A|B|SIR)-?
    (?P<effective_date>FA\d+(GEN|SUPP)?)-
    (?P<unknown_1>.+)-
    (?P<service_code>Weekday|Saturday|Sunday)-
    (?P<unknown_2>\d{2})_
    (?P<origin_time>\d{6})_
    (?P<trip_path>
        (?P<route_id_and_dots>.+)\.{1,2}
        (?P<direction>[NSEW])
        (?P<path_identifier>.*)
    )
"""


def parse_trip_id(trip_id: str) -> Optional[TripID]:
    m = re.fullmatch(TRIP_ID_RE, trip_id, re.VERBOSE)
    if m is None:
        raise Exception("Unable to parse trip_id {}".format(trip_id))

    route_id = m.group("route_id_and_dots").rstrip(".")

    # see if our guess about the unknown bit is correct
    assert m.group("unknown_1").startswith(route_id), trip_id

    return TripID(
        m.group("sub_division"),
        m.group("effective_date"),
        ServiceDay(m.group("service_code")),
        m.group("origin_time"),
        m.group("trip_path"),
        route_id,
        m.group("direction"),
        m.group("path_identifier"),
    )


def short_trip_ids(trip_id: str) -> List[str]:
    """
    Gets possible short trip IDs from the original trip ID.  The realtime API
    usse this as its trip ID instead of the full string.  This is based on
    origin time, route id, and direction, and possibly path identifier.  It
    looks like (origin time, route id, direction) is not always unique (it's
    supposed to be) so in some cases path identifier is also used.

    Example: "AFA19GEN-1037-Sunday-00_010600_1..S03R" ->
        ["010600_1..S", "010600_1..S03R"]
    """
    parsed = parse_trip_id(trip_id)
    return [
        "{}_{}".format(parsed.origin_time, parsed.trip_path),
        "{}_{}".format(
            parsed.origin_time,
            # Trip path without path identifier
            parsed.trip_path[: -len(parsed.path_identifier)],
        ),
    ]
