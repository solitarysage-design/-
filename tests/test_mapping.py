import pandas as pd

from main import compute_leverage_metrics, detect_column


def test_detect_column_fuzzy():
    df = pd.DataFrame([{"cash_eq": 10, "interest_bearing_debt_total": 20}])
    assert detect_column(df, ["CashAndCashEquivalents"]) == "cash_eq"


def test_compute_leverage_metrics():
    df = pd.DataFrame([
        {
            "InterestBearingDebt": 300,
            "CashAndCashEquivalents": 100,
            "OperatingProfit": 80,
            "DepreciationAmortization": 20,
            "InterestExpense": 10,
        }
    ])
    nde, ic, reasons = compute_leverage_metrics(df)
    assert round(nde, 2) == 2.0
    assert round(ic, 2) == 8.0
    assert reasons == []
