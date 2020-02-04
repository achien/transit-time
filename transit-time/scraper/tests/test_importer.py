from datetime import datetime

import pytest

from scraper.importer import CartesianProduct, get_db_rows

NOW = datetime.now()
DB_ROWS_PARAMS = [
    (
        {"num": 1, "str": "a", "datetime": NOW},
        [{"num": 1, "str": "a", "datetime": NOW}],
    ),
    (
        {"num": CartesianProduct([1, 2]), "str": "a", "datetime": NOW},
        [
            {"num": 1, "str": "a", "datetime": NOW},
            {"num": 2, "str": "a", "datetime": NOW},
        ],
    ),
    (
        {
            "num": CartesianProduct([1, 2]),
            "str": CartesianProduct(["a", "b"]),
            "datetime": NOW,
        },
        [
            {"num": 1, "str": "a", "datetime": NOW},
            {"num": 2, "str": "a", "datetime": NOW},
            {"num": 1, "str": "b", "datetime": NOW},
            {"num": 2, "str": "b", "datetime": NOW},
        ],
    ),
]


@pytest.mark.parametrize("spec,rows", DB_ROWS_PARAMS)
def test_get_db_rows(spec, rows):
    def cmp_key(d):
        sorted(d.items())

    assert sorted(get_db_rows(spec), key=cmp_key) == sorted(rows, key=cmp_key)
