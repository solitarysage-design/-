#!/usr/bin/env python3
from __future__ import annotations
import argparse
import gzip
import io
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

BASE_URL = "https://api.jquants.com/v2"
OUTPUT_CSV = "output/screen_results.csv"


def setup_logger() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class JQuantsClient:
    def __init__(self, api_key: str, max_retries: int = 4, backoff_seconds: float = 1.2):
        import requests

        self.requests = requests
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": api_key})
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def _request(self, method: str, path: str, **kwargs):
        url = f"{BASE_URL}{path}"
        for i in range(self.max_retries):
            try:
                r = self.session.request(method, url, timeout=40, **kwargs)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise self.requests.HTTPError(f"retryable status={r.status_code}")
                r.raise_for_status()
                return r
            except Exception as exc:
                if i == self.max_retries - 1:
                    raise
                sleep_for = self.backoff_seconds * (2**i)
                logging.warning("request failed (%s), retry in %.1fs: %s", path, sleep_for, exc)
                time.sleep(sleep_for)
        raise RuntimeError("unreachable")

    def bulk_csv(self, endpoint: str):
        import pandas as pd

        key_resp = self._request("GET", "/bulk/list", params={"endpoint": endpoint}).json()
        key = key_resp.get("Key") or key_resp.get("key")
        if not key:
            raise RuntimeError(f"bulk/list missing Key for endpoint={endpoint}: {key_resp}")

        get_resp = self._request("GET", "/bulk/get", params={"key": key}).json()
        dl_url = get_resp.get("url") or get_resp.get("URL")
        if not dl_url:
            raise RuntimeError(f"bulk/get missing url for endpoint={endpoint}: {get_resp}")

        if dl_url.startswith(BASE_URL):
            raw = self._request("GET", dl_url.replace(BASE_URL, ""))
        else:
            raw = self.requests.get(dl_url, timeout=60)
            raw.raise_for_status()

        content = raw.content
        if dl_url.endswith(".gz") or raw.headers.get("content-type", "").startswith("application/gzip"):
            content = gzip.decompress(content)
        return pd.read_csv(io.BytesIO(content))

    def fins_details_latest(self, code: str):
        import pandas as pd

        resp = self._request("GET", "/fins/details", params={"code": code}).json()
        rows = resp.get("fins_details") or resp.get("finsDetails") or resp.get("list") or []
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        date_col = detect_column(df, ["DisclosedDate", "Date", "CurrentPeriodEndDate"])
        if date_col:
            df = df.sort_values(date_col).tail(1)
        return df


def detect_column(df, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    for c in candidates:
        if c in cols:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in candidates:
        key = c.lower().replace("_", "")
        for col in cols:
            col_key = col.lower().replace("_", "")
            if key in col_key or col_key in key:
                return col
    return None


def pick_col(df, group_name: str, aliases: List[str]) -> Optional[str]:
    col = detect_column(df, aliases)
    if col:
        logging.info("details mapping: %s -> %s", group_name, col)
    else:
        logging.info("details mapping: %s -> not found", group_name)
    return col


def safe_float(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(v)
    except Exception:
        return None


def cagr(series: List[Optional[float]], years: int) -> Optional[float]:
    if len(series) < years or series[0] is None or series[-1] is None or series[0] <= 0 or series[-1] <= 0:
        return None
    return (series[-1] / series[0]) ** (1 / (years - 1)) - 1


def rolling_mean(values: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


def fmt_yen(v: Any) -> str:
    x = safe_float(v)
    return f"¥{x:,.0f}" if x is not None else "NA"


def fmt_pct(v: Any, digits: int = 1) -> str:
    x = safe_float(v)
    return f"{x * 100:.{digits}f}%" if x is not None else "NA"


def fmt_num(v: Any, digits: int = 2) -> str:
    x = safe_float(v)
    return f"{x:.{digits}f}" if x is not None else "NA"


@dataclass
class Metrics:
    code: str
    name: str
    market: str
    price_date: Optional[str]
    price: Optional[float]
    dividend_yield: Optional[float]
    per: Optional[float]
    pbr: Optional[float]
    fcf_yield: Optional[float]
    eps_cagr_5y: Optional[float]
    dps_cagr_5y: Optional[float]
    profit_payout_3y: Optional[float]
    fcf_payout_3y: Optional[float]
    profit_cover_3y: Optional[float]
    fcf_cover_3y: Optional[float]
    net_debt_ebitda: Optional[float]
    interest_coverage: Optional[float]
    score: int
    signals: str
    buy_zone_low: Optional[float]
    buy_zone_high: Optional[float]
    na_reason: str


def compute_leverage_metrics(details_df) -> Tuple[Optional[float], Optional[float], List[str]]:
    if details_df.empty:
        return None, None, ["details_missing"]
    debt_col = pick_col(details_df, "debt", ["InterestBearingDebt", "TotalInterestBearingDebt", "BondsAndLoansPayable"])
    cash_col = pick_col(details_df, "cash", ["CashAndCashEquivalents", "CashEq", "CashAndDeposits"])
    op_col = pick_col(details_df, "operating_profit", ["OperatingProfit", "OperatingIncome"])
    dep_col = pick_col(details_df, "depreciation", ["DepreciationAmortization", "Depreciation", "DepreciationAndAmortization"])
    int_col = pick_col(details_df, "interest_expense", ["InterestExpense", "FinanceCosts", "InterestAndDividendExpenses"])

    reasons: List[str] = []
    debt = safe_float(details_df.iloc[0][debt_col]) if debt_col else None
    cash = safe_float(details_df.iloc[0][cash_col]) if cash_col else None
    op = safe_float(details_df.iloc[0][op_col]) if op_col else None
    dep = safe_float(details_df.iloc[0][dep_col]) if dep_col else None
    interest = safe_float(details_df.iloc[0][int_col]) if int_col else None

    net_debt_ebitda = None
    if debt is not None and cash is not None and op is not None and dep is not None and (op + dep) > 0:
        net_debt_ebitda = (debt - cash) / (op + dep)
    else:
        reasons.append("netdebt_ebitda_unavailable")

    interest_cov = None
    if op is not None and interest is not None and interest > 0:
        interest_cov = op / interest
    else:
        reasons.append("interest_coverage_unavailable")

    return net_debt_ebitda, interest_cov, reasons


def passes_value_rules(metric: Metrics, rules: Dict[str, Any]) -> bool:
    signals = []
    if metric.dividend_yield is not None and rules["dividend_yield_low"] <= metric.dividend_yield <= rules["dividend_yield_high"]:
        signals.append("YieldZone")
    if metric.fcf_yield is not None and metric.fcf_yield >= rules["fcf_yield_min"]:
        signals.append("FCFYield")
    if metric.per is not None and metric.per <= rules["per_max"]:
        signals.append("PER")
    if metric.pbr is not None and metric.pbr <= rules["pbr_max"]:
        signals.append("PBR")
    metric.signals = "|".join(signals)
    return len(signals) >= rules["min_signals_to_pass"]


def evaluate_code(code: str, master_row, bars, fins, config: dict, client: Optional[JQuantsClient]) -> Tuple[Optional[Metrics], bool]:
    import pandas as pd

    thr = config["thresholds"]
    na_reasons: List[str] = []

    code_fins = fins[fins["Code"].astype(str) == str(code)].sort_values("DisclosedDate").tail(5)
    if len(code_fins) < 3:
        return None, False

    eps = [safe_float(x) for x in code_fins.get("EPS", pd.Series([None] * len(code_fins))).tolist()]
    dps = [safe_float(x) for x in code_fins.get("DPS", pd.Series([None] * len(code_fins))).tolist()]
    cfo = [safe_float(x) for x in code_fins.get("CashFlowsFromOperatingActivities", pd.Series([None] * len(code_fins))).tolist()]
    cfi = [safe_float(x) for x in code_fins.get("CashFlowsFromInvestingActivities", pd.Series([None] * len(code_fins))).tolist()]
    profit_payout = [safe_float(x) for x in code_fins.get("DividendPayoutRatio", pd.Series([None] * len(code_fins))).tolist()]

    fcfs = [(a + b) if a is not None and b is not None else None for a, b in zip(cfo, cfi)]
    fcf_payout, profit_cover, fcf_cover = [], [], []
    for idx, row in code_fins.iterrows():
        div = safe_float(row.get("DividendPaid")) or safe_float(row.get("AnnualDividendPerShare"))
        ni = safe_float(row.get("Profit")) or safe_float(row.get("NetIncome"))
        fcf = fcfs[list(code_fins.index).index(idx)]
        profit_cover.append((ni / div) if (ni is not None and div and div > 0) else None)
        fcf_cover.append((fcf / div) if (fcf is not None and div and div > 0) else None)
        fcf_payout.append((div / fcf) if (div is not None and fcf and fcf > 0) else None)

    eps_cagr = cagr(eps, min(5, len(eps)))
    dps_cagr = cagr(dps, min(5, len(dps)))

    if sum(1 for x in cfo if x is not None and x > 0) < thr["min_operating_cf_positive_years"]:
        return None, False
    if sum(1 for x in fcfs if x is not None and x > 0) < thr["min_fcf_positive_years"]:
        return None, False
    if eps_cagr is None or eps_cagr < thr["eps_cagr_min"]:
        return None, False

    fcf_payout_3y = rolling_mean(fcf_payout[-3:])
    profit_payout_3y = rolling_mean(profit_payout[-3:])
    profit_cover_3y = rolling_mean(profit_cover[-3:])
    fcf_cover_3y = rolling_mean(fcf_cover[-3:])

    sensitive = str(code) in set(map(str, config.get("sector_sensitive_codes", [])))
    fcf_limit = thr["fcf_payout_max_sensitive"] if sensitive else thr["fcf_payout_max"]
    if fcf_payout_3y is None or fcf_payout_3y > fcf_limit:
        return None, False
    if profit_payout_3y is None or profit_payout_3y > thr["profit_payout_max"]:
        return None, False
    if profit_cover_3y is None or profit_cover_3y < thr["profit_cover_min"]:
        return None, False
    if fcf_cover_3y is None or fcf_cover_3y < thr["fcf_cover_min"]:
        return None, False

    net_debt_ebitda = None
    interest_cov = None
    if client:
        details_df = client.fins_details_latest(str(code))
        net_debt_ebitda, interest_cov, reasons = compute_leverage_metrics(details_df)
        na_reasons.extend(reasons)

    strict = config.get("strict_mode", True)
    utility_like = str(code) in set(map(str, config.get("utility_like_codes", [])))
    nde_limit = thr["net_debt_ebitda_max_utility"] if utility_like else thr["net_debt_ebitda_max"]
    if strict:
        if net_debt_ebitda is None or interest_cov is None:
            return None, False
        if net_debt_ebitda > nde_limit or interest_cov < thr["interest_coverage_min"]:
            return None, False
    else:
        if net_debt_ebitda is not None and net_debt_ebitda > nde_limit:
            return None, False
        if interest_cov is not None and interest_cov < thr["interest_coverage_min"]:
            return None, False

    bar = bars[bars["Code"].astype(str) == str(code)].sort_values("Date").tail(1)
    price = safe_float(bar.iloc[0].get("AdjustmentClose")) if not bar.empty else None
    if price is None and not bar.empty:
        price = safe_float(bar.iloc[0].get("Close"))
    price_date = str(bar.iloc[0].get("Date")) if not bar.empty else None

    latest = code_fins.iloc[-1]
    market_cap = safe_float(latest.get("MarketCapitalization"))
    fcf_latest = fcfs[-1] if fcfs else None

    metric = Metrics(
        code=str(code),
        name=str(master_row.get("Name") or master_row.get("CompanyName") or ""),
        market=str(master_row.get("MarketCodeName") or master_row.get("Market") or ""),
        price_date=price_date,
        price=price,
        dividend_yield=safe_float(latest.get("DividendYield")),
        per=safe_float(latest.get("PER")),
        pbr=safe_float(latest.get("PBR")),
        fcf_yield=(fcf_latest / market_cap) if (fcf_latest is not None and market_cap and market_cap > 0) else None,
        eps_cagr_5y=eps_cagr,
        dps_cagr_5y=dps_cagr,
        profit_payout_3y=profit_payout_3y,
        fcf_payout_3y=fcf_payout_3y,
        profit_cover_3y=profit_cover_3y,
        fcf_cover_3y=fcf_cover_3y,
        net_debt_ebitda=net_debt_ebitda,
        interest_coverage=interest_cov,
        score=0,
        signals="",
        buy_zone_low=(price * 0.9 if price else None),
        buy_zone_high=(price * 1.02 if price else None),
        na_reason=";".join(sorted(set(na_reasons))),
    )

    if dps_cagr is not None and dps_cagr >= 0.03:
        metric.score += config["soft_weights"]["dps_cagr_3pct"]
    if dps_cagr is not None and dps_cagr >= 0.05:
        metric.score += config["soft_weights"]["dps_cagr_5pct"]
    sh = [safe_float(x) for x in code_fins.get("ShOutFY", pd.Series([None] * len(code_fins))).tolist()]
    if len(sh) >= 3 and all(x is not None for x in sh[-3:]) and sh[-1] < sh[-3]:
        metric.score += config["soft_weights"]["share_count_reduction"]
    roic = safe_float(latest.get("ROIC"))
    roe = safe_float(latest.get("ROE"))
    if (roic is not None and roic >= 0.08) or (roe is not None and roe >= 0.10):
        metric.score += config["soft_weights"]["roic_or_roe"]
    cfo_vals = [x for x in cfo if x is not None]
    if len(cfo_vals) >= 5:
        cfo_series = pd.Series(cfo_vals)
        cfo_std = cfo_series.std()
        cfo_mean = cfo_series.mean()
        if cfo_mean and cfo_std is not None and abs(cfo_std / cfo_mean) < 0.8:
            metric.score += config["soft_weights"]["cf_margin_stability"]

    return metric, True


def to_df(rows: List[Metrics]):
    import pandas as pd

    data = [
        {
            "Code": r.code,
            "Name": r.name,
            "Mkt": r.market,
            "PxDate": r.price_date,
            "Px": r.price,
            "Yield": r.dividend_yield,
            "PER": r.per,
            "PBR": r.pbr,
            "FCFYield": r.fcf_yield,
            "Signals": r.signals,
            "Score": r.score,
            "BuyLow": r.buy_zone_low,
            "BuyHigh": r.buy_zone_high,
            "EPS_CAGR_5Y": r.eps_cagr_5y,
            "DPS_CAGR_5Y": r.dps_cagr_5y,
            "Profit_Payout_3Y": r.profit_payout_3y,
            "FCF_Payout_3Y": r.fcf_payout_3y,
            "Profit_Cover_3Y": r.profit_cover_3y,
            "FCF_Cover_3Y": r.fcf_cover_3y,
            "NetDebtEBITDA": r.net_debt_ebitda,
            "InterestCoverage": r.interest_coverage,
            "NA_Reason": r.na_reason,
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    fixed = [
        "Code",
        "Name",
        "Mkt",
        "PxDate",
        "Px",
        "Yield",
        "PER",
        "PBR",
        "FCFYield",
        "Signals",
        "Score",
        "BuyLow",
        "BuyHigh",
        "EPS_CAGR_5Y",
        "DPS_CAGR_5Y",
        "Profit_Payout_3Y",
        "FCF_Payout_3Y",
        "Profit_Cover_3Y",
        "FCF_Cover_3Y",
        "NetDebtEBITDA",
        "InterestCoverage",
        "NA_Reason",
    ]
    if df.empty:
        return pd.DataFrame(columns=fixed)
    rest = [c for c in df.columns if c not in fixed]
    return df[[c for c in fixed if c in df.columns] + rest]


def build_hard_reason_lines(row: Dict[str, Any]) -> List[str]:
    lines = [
        "非減配(5年)の前提でHard通過",
        "営業CF：5年中4年以上プラス",
        "FCF：5年中3年以上プラス",
        f"FCF配当性向(3Y) {fmt_pct(row.get('FCF_Payout_3Y'))}",
        f"利益カバー(3Y) {fmt_num(row.get('Profit_Cover_3Y'), 2)}倍",
        f"EPS CAGR(5Y) {fmt_pct(row.get('EPS_CAGR_5Y'))}",
    ]
    return lines[:6]


def build_slack_blocks(df, meta: Dict[str, Any], top_n: int) -> List[Dict[str, Any]]:
    import pandas as pd

    top = df.head(top_n)
    blocks: List[Dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": f"増配バリュー株スクリーニング結果（通過{meta['final_passed_count']}）"}},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"価格基準日: {meta.get('price_date') or 'NA'} | Universe: {meta['universe_count']} | "
                        f"Hard通過: {meta['hard_passed_count']} | 最終通過: {meta['final_passed_count']} | NA件数: {meta['na_count']}"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]

    overview_lines = []
    for _, r in top.iterrows():
        bz = f"{fmt_yen(r.get('BuyLow'))}–{fmt_yen(r.get('BuyHigh'))}" if pd.notna(r.get("BuyLow")) else "NA"
        overview_lines.append(
            f"• *{r['Code']} {r['Name']}* / Y:{fmt_pct(r.get('Yield'))} PER:{fmt_num(r.get('PER'),1)} PBR:{fmt_num(r.get('PBR'),2)} "
            f"FCFy:{fmt_pct(r.get('FCFYield'))} S:{int(r.get('Score', 0))} Sig:{r.get('Signals','-')} BZ:{bz}"
        )
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*俯瞰（TopN）*\n" + ("\n".join(overview_lines) if overview_lines else "該当なし")}})
    blocks.append({"type": "divider"})

    down_triggers = ["配当方針の後退・減配示唆", "営業CF/FCFの悪化継続", "レバレッジ悪化（有利子負債増）"]
    up_triggers = ["増配方針の明確化", "利益率改善とCF拡大", "株主還元（自社株買い）強化"]

    for _, r in top.iterrows():
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{r['Code']} {r['Name']}*  | Score:{int(r.get('Score',0))} | Signals:{r.get('Signals','-')}"},
                "fields": [
                    {"type": "mrkdwn", "text": f"*株価*\n{fmt_yen(r.get('Px'))}"},
                    {"type": "mrkdwn", "text": f"*利回り*\n{fmt_pct(r.get('Yield'))}"},
                    {"type": "mrkdwn", "text": f"*PER/PBR*\n{fmt_num(r.get('PER'),1)} / {fmt_num(r.get('PBR'),2)}"},
                    {"type": "mrkdwn", "text": f"*FCF利回り*\n{fmt_pct(r.get('FCFYield'))}"},
                    {"type": "mrkdwn", "text": f"*DPS CAGR(5Y)*\n{fmt_pct(r.get('DPS_CAGR_5Y'))}"},
                    {"type": "mrkdwn", "text": f"*BuyZone*\n{fmt_yen(r.get('BuyLow'))}–{fmt_yen(r.get('BuyHigh'))}"},
                ],
            }
        )
        reasons = "\n".join([f"• {x}" for x in build_hard_reason_lines(r.to_dict())])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*通過理由（Hard要点）*\n{reasons}"}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*下げトリガー*\n" + "\n".join([f"• {x}" for x in down_triggers])}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*上げトリガー*\n" + "\n".join([f"• {x}" for x in up_triggers])}})
        blocks.append({"type": "divider"})

    return blocks


def post_to_slack(webhook_url: str, df, meta: Dict[str, Any], top_n: int) -> None:
    import requests

    blocks = build_slack_blocks(df, meta, top_n)
    payload = {
        "text": f"増配バリュー株スクリーニング結果（通過{meta['final_passed_count']}）",
        "blocks": blocks,
    }
    resp = requests.post(webhook_url, json=payload, timeout=20)
    resp.raise_for_status()


def run(args) -> int:
    setup_logger()
    os.makedirs("output", exist_ok=True)

    if args.dry_run:
        import csv

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Code", "Name", "Score", "Signals", "NA_Reason"])
            w.writeheader()
            w.writerow({"Code": "0000", "Name": "DRY", "Score": 0, "Signals": "DRY", "NA_Reason": "dry_run"})
        logging.info("dry-run complete, wrote %s", OUTPUT_CSV)
        return 0

    import yaml

    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    api_key = os.getenv("JQUANTS_API_KEY")
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY is required unless --dry-run")

    client = JQuantsClient(api_key, config.get("max_retries", 4), config.get("backoff_seconds", 1.2))
    master = client.bulk_csv("/equities/master")
    bars = client.bulk_csv("/equities/bars/daily")
    fins = client.bulk_csv("/fins/summary")

    for col in ["Code", "DisclosedDate"]:
        if col not in fins.columns:
            raise RuntimeError(f"fins summary missing required column: {col}")

    if args.max_codes:
        master = master.head(args.max_codes)

    universe_count = len(master)
    hard_passed_count = 0
    final_rows: List[Metrics] = []

    for _, m in master.iterrows():
        code = str(m.get("Code"))
        if not code or code == "nan":
            continue
        try:
            metric, hard_pass = evaluate_code(code, m, bars, fins, config, client)
            if hard_pass:
                hard_passed_count += 1
            if metric and passes_value_rules(metric, config["value_rules"]):
                final_rows.append(metric)
        except Exception as exc:
            logging.warning("code=%s skipped due to error: %s", code, exc)

    df = to_df(final_rows)
    if not df.empty:
        df = df.sort_values(["Score", "Yield"], ascending=[False, False])
    df.to_csv(OUTPUT_CSV, index=False)

    final_passed_count = len(df)
    na_count = int((df["NA_Reason"].fillna("") != "").sum()) if "NA_Reason" in df.columns else 0
    price_date = str(df["PxDate"].dropna().max()) if ("PxDate" in df.columns and not df["PxDate"].dropna().empty) else None
    top_n = config.get("top_n", 10)

    logging.info(
        "取得件数=%d -> Hard通過件数=%d -> 価値条件通過件数=%d -> TopN=%d",
        universe_count,
        hard_passed_count,
        final_passed_count,
        min(top_n, final_passed_count),
    )

    meta = {
        "universe_count": universe_count,
        "hard_passed_count": hard_passed_count,
        "final_passed_count": final_passed_count,
        "na_count": na_count,
        "price_date": price_date,
    }

    slack_enabled = config.get("post_to_slack", True) and not args.no_slack
    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if slack_enabled and webhook and not df.empty:
        try:
            post_to_slack(webhook, df, meta, top_n)
            logging.info("Slack posted top %d", min(top_n, len(df)))
        except Exception as exc:
            logging.warning("Slack post failed (CSV is preserved): %s", exc)

    return 0


def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-slack", action="store_true")
    p.add_argument("--max-codes", type=int, default=0)
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_arg_parser().parse_args()))
