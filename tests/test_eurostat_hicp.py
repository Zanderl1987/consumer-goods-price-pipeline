
from eurostat_hicp_pipeline import parse_jsonstat


def test_parse_jsonstat_flattens_cube():
    payload = {
        "id": ["freq", "unit", "coicop", "geo", "time"],
        "size": [1, 1, 1, 2, 2],
        "dimension": {
            "freq": {"category": {"index": {"M": 0}}},
            "unit": {"category": {"index": {"I15": 0}}},
            "coicop": {"category": {"index": {"CP00": 0}}},
            "geo": {"category": {"index": {"DE": 0, "FR": 1}}},
            "time": {"category": {"index": {"2023M01": 0, "2023M02": 1}}},
        },
        "value": {"0": 102.5, "1": 103.0, "2": 100.5, "3": 101.0},
    }
    df = parse_jsonstat(payload)
    assert len(df) == 4
    assert set(df["geo"]) == {"DE", "FR"}
    assert set(df["date"]) == {"2023M01", "2023M02"}
    assert set(df["coicop"]) == {"CP00"}
    assert df["value"].tolist() == [102.5, 103.0, 100.5, 101.0]


def test_parse_jsonstat_handles_empty():
    assert parse_jsonstat({}).empty
    assert parse_jsonstat({"value": {}, "id": [], "size": []}).empty
