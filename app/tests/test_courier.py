import pandas as pd

from cartpulse.batch.courier import standardise, validate

COLS = ["waybill", "order_id", "courier", "province", "collected_at", "delivered_at", "status", "attempts"]


def frame(rows):
    return pd.DataFrame(rows, columns=COLS)


GOOD = ["WB1", "10", "SwiftRoute", "Gauteng", "2026-09-28 09:00:00", "2026-09-29 15:00:00", "DELIVERED", "1"]


def test_fixable_problems_are_repaired():
    df = standardise(frame([["WB2", "11", "SwiftRoute", "Gauteng", "2026-09-28 09:00:00", "29/09/2026 15:00",
                             "delivered", "1"]]))
    good, rejected, _ = validate(df)
    assert len(good) == 1 and rejected.empty and good.iloc[0]["status"] == "DELIVERED"


def test_bad_rows_are_quarantined_with_reasons():
    rows = [
        GOOD,
        ["WB3", "", *GOOD[2:]],                                                       # no order
        ["WB4", "12", "SwiftRoute", "Gauteng", "2026-09-28 09:00:00", "2026-09-27 09:00:00", "DELIVERED", "1"],
        ["WB5", "13", "SwiftRoute", "Gauteng", "2026-09-28 09:00:00", "", "DELIVERED", "1"],
        ["WB6", "14", "SwiftRoute", "Gauteng", "2026-09-28 09:00:00", "", "LOST", "1"],
    ]
    good, rejected, results = validate(standardise(frame(rows)))
    assert list(good["waybill"]) == ["WB1"]
    assert dict(zip(rejected["waybill"], rejected["failed_rules"])) == {
        "WB3": "order_id_present", "WB4": "delivered_after_collected",
        "WB5": "delivered_has_timestamp", "WB6": "status_known"}
    assert {r["dimension"] for r in results} == {"completeness", "validity", "consistency", "uniqueness"}


def test_duplicates_are_dropped_and_counted():
    good, _, results = validate(standardise(frame([GOOD, GOOD])))
    assert len(good) == 1
    assert next(r for r in results if r["rule"] == "unique_waybill_status")["failed"] == 1
