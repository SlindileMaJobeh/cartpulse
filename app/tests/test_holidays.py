from datetime import date

import pytest

from cartpulse.batch import holidays

PAYLOAD = [{"date": "2026-09-24", "localName": "Heritage Day", "name": "Heritage Day", "countryCode": "ZA"},
           {"date": "2026-12-16", "localName": "Day of Reconciliation", "name": "Day of Reconciliation", "countryCode": "ZA"}]


def test_parse():
    df = holidays.parse(PAYLOAD)
    assert list(df["holiday_date"]) == [date(2026, 9, 24), date(2026, 12, 16)]


def test_parse_rejects_other_country():
    with pytest.raises(ValueError):
        holidays.parse([{**PAYLOAD[0], "countryCode": "NA"}])


def test_falls_back_when_api_down(monkeypatch):
    monkeypatch.setattr(holidays, "fetch_year", lambda y: (_ for _ in ()).throw(RuntimeError("down")))
    df = holidays.collect([2026])
    assert set(df["source"]) == {"fallback"} and date(2026, 9, 24) in set(df["holiday_date"])
