import pytest

from scraper import nyc

PARSE_TRIP_ID_ARGNAMES = [
    "trip_id",
    "sub_division",
    "effective_date",
    "service_day",
    "origin_time",
    "trip_path",
    "route_id",
    "direction",
    "path_identifier",
]
PARSE_TRIP_ID_PARAMS = [
    (
        "AFA19GEN-1037-Sunday-00_010600_1..S03R",
        "A",
        "FA19GEN",
        nyc.ServiceDay.SUNDAY,
        "010600",
        "1..S03R",
        "1",
        "S",
        "03R",
    ),
    (
        "AFA19GEN-GS010-Saturday-00_036400_GS.S01R",
        "A",
        "FA19GEN",
        nyc.ServiceDay.SATURDAY,
        "036400",
        "GS.S01R",
        "GS",
        "S",
        "01R",
    ),
    (
        "BFA19GEN-N058-Sunday-00_096300_N..S36R",
        "B",
        "FA19GEN",
        nyc.ServiceDay.SUNDAY,
        "096300",
        "N..S36R",
        "N",
        "S",
        "36R",
    ),
    (
        "BFA19SUPP-L024-Sunday-99_090000_L..N01R",
        "B",
        "FA19SUPP",
        nyc.ServiceDay.SUNDAY,
        "090000",
        "L..N01R",
        "L",
        "N",
        "01R",
    ),
    (
        "SIR-FA2017-SI017-Weekday-08_121100_SI..N03R",
        "SIR",
        "FA2017",
        nyc.ServiceDay.WEEKDAY,
        "121100",
        "SI..N03R",
        "SI",
        "N",
        "03R",
    ),
]


@pytest.mark.parametrize(",".join(PARSE_TRIP_ID_ARGNAMES), PARSE_TRIP_ID_PARAMS)
def test_parse_trip_id(
    trip_id,
    sub_division,
    effective_date,
    service_day,
    origin_time,
    trip_path,
    route_id,
    direction,
    path_identifier,
):
    parsed = nyc.parse_trip_id(trip_id)
    assert parsed.sub_division == sub_division
    assert parsed.effective_date == effective_date
    assert parsed.service_day == service_day
    assert parsed.origin_time == origin_time
    assert parsed.trip_path == trip_path
    assert parsed.route_id == route_id
    assert parsed.direction == direction
    assert parsed.path_identifier == path_identifier


SHORT_TRIP_ID_PARAMS = [
    ("AFA19GEN-1037-Sunday-00_010600_1..S03R", ["010600_1..S", "010600_1..S03R"]),
    ("AFA19GEN-GS010-Saturday-00_036400_GS.S01R", ["036400_GS.S", "036400_GS.S01R"]),
    ("BFA19GEN-N058-Sunday-00_096300_N..S36R", ["096300_N..S", "096300_N..S36R"]),
    ("BFA19SUPP-L024-Sunday-99_090000_L..N01R", ["090000_L..N", "090000_L..N01R"]),
    (
        "SIR-FA2017-SI017-Weekday-08_121100_SI..N03R",
        ["121100_SI..N", "121100_SI..N03R"],
    ),
]


@pytest.mark.parametrize("trip_id,short_trip_ids", SHORT_TRIP_ID_PARAMS)
def test_short_trip_id(trip_id, short_trip_ids):
    assert set(nyc.short_trip_ids(trip_id)) == set(short_trip_ids)
