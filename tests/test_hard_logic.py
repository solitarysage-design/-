import pandas as pd

from main import evaluate_code, passes_value_rules


def test_hard_logic_pass_with_non_strict():
    master_row = pd.Series({"Code": "1111", "Name": "A", "Market": "Prime"})
    bars = pd.DataFrame([{"Code": "1111", "Date": "2024-01-01", "Close": 1000}])
    fins = pd.DataFrame([
        {"Code": "1111", "DisclosedDate": "2020-03-31", "EPS": 100, "DPS": 30, "CashFlowsFromOperatingActivities": 1000, "CashFlowsFromInvestingActivities": -200, "DividendPayoutRatio": 0.4, "DividendPaid": 30, "Profit": 100, "PER": 10, "PBR": 1.0, "DividendYield": 0.04, "MarketCapitalization": 10000},
        {"Code": "1111", "DisclosedDate": "2021-03-31", "EPS": 103, "DPS": 31, "CashFlowsFromOperatingActivities": 1100, "CashFlowsFromInvestingActivities": -250, "DividendPayoutRatio": 0.4, "DividendPaid": 31, "Profit": 105, "PER": 10, "PBR": 1.0, "DividendYield": 0.04, "MarketCapitalization": 10000},
        {"Code": "1111", "DisclosedDate": "2022-03-31", "EPS": 106, "DPS": 32, "CashFlowsFromOperatingActivities": 1200, "CashFlowsFromInvestingActivities": -250, "DividendPayoutRatio": 0.4, "DividendPaid": 32, "Profit": 110, "PER": 10, "PBR": 1.0, "DividendYield": 0.04, "MarketCapitalization": 10000},
        {"Code": "1111", "DisclosedDate": "2023-03-31", "EPS": 109, "DPS": 33, "CashFlowsFromOperatingActivities": 1300, "CashFlowsFromInvestingActivities": -280, "DividendPayoutRatio": 0.4, "DividendPaid": 33, "Profit": 115, "PER": 10, "PBR": 1.0, "DividendYield": 0.04, "MarketCapitalization": 10000},
        {"Code": "1111", "DisclosedDate": "2024-03-31", "EPS": 113, "DPS": 34, "CashFlowsFromOperatingActivities": 1400, "CashFlowsFromInvestingActivities": -300, "DividendPayoutRatio": 0.4, "DividendPaid": 34, "Profit": 120, "PER": 10, "PBR": 1.0, "DividendYield": 0.04, "MarketCapitalization": 10000},
    ])
    config = {
        "thresholds": {
            "min_operating_cf_positive_years": 4,
            "min_fcf_positive_years": 3,
            "eps_cagr_min": 0.03,
            "fcf_payout_max": 0.7,
            "fcf_payout_max_sensitive": 0.6,
            "profit_payout_max": 0.6,
            "profit_cover_min": 1.5,
            "fcf_cover_min": 1.2,
            "net_debt_ebitda_max": 2.0,
            "net_debt_ebitda_max_utility": 3.0,
            "interest_coverage_min": 5.0,
        },
        "strict_mode": False,
        "soft_weights": {"dps_cagr_3pct": 1, "dps_cagr_5pct": 1, "share_count_reduction": 1, "roic_or_roe": 1, "cf_margin_stability": 1},
        "value_rules": {"dividend_yield_low": 0.032, "dividend_yield_high": 0.055, "fcf_yield_min": 0.05, "per_max": 14, "pbr_max": 1.2, "min_signals_to_pass": 2},
        "sector_sensitive_codes": [],
        "utility_like_codes": [],
    }
    metric, hard_pass = evaluate_code("1111", master_row, bars, fins, config, None)
    assert hard_pass is True
    assert metric is not None
    assert passes_value_rules(metric, config["value_rules"]) is True
