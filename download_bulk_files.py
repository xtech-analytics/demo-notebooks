"""
Download bulk files without replicating features.

This script efficiently downloads large volumes of files while ensuring no duplicate
features are downloaded. It provides robust handling for resumable downloads and
comprehensive logging to track progress.

What it does:
- Downloads files in bulk from specified sources
- Prevents duplicate feature downloads by tracking already downloaded files
- Supports resumable downloads; existing files are skipped unless --overwrite is given
- Provides detailed logging with progress tracking, file counts, and timing information

Usage:
    python download_bulk_files.py --source <source_dir> --output <target_dir> \
        --user <username> --token <token>

Dependencies: pandas, requests, pyarrow, and related utility packages.

Note: Check the final summary for any skipped or failed files to ensure completeness.
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pandas_market_calendars as pmc
import requests

from unifier import unifier as Unifier

logger = logging.getLogger(__name__)

QUERY_URL = "https://unifier.exponential-tech.ai/unifier"
DEFAULT_TIMEOUT = 300  # seconds per query; 1-min tables can be slow, override with --timeout
RETRIES = 3
RETRY_WAIT_SECONDS = 5
CALENDAR = "XNYS"  # NYSE


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def set_credentials(user: str, token: str) -> None:
    Unifier.user = user
    Unifier.token = token
    logger.info("Using Unifier credentials for user=%s", Unifier.user)


def next_day(d: date) -> str:
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as e.g. 45s / 3m05s / 1h02m03s."""
    seconds = round(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def parse_rows(data: object) -> pd.DataFrame:
    """Normalize the raw Unifier response (list of rows; each row a list of {col: value}
    dicts, or a plain dict) into a DataFrame."""
    if not data:
        return pd.DataFrame()
    if isinstance(data, dict):
        return pd.DataFrame.from_dict(data)
    rows = []
    for item in data:  # type: ignore[union-attr] - data is a list here
        if isinstance(item, dict):
            rows.append(item)
        else:
            rows.append({k: v for d in item for k, v in d.items()})
    return pd.DataFrame(rows)


def fetch_df(
    table_name: str,
    back_to: str,
    up_to: str,
    timeout: int = DEFAULT_TIMEOUT,
    retry_on_empty: bool = False,
) -> pd.DataFrame:
    """Query one date range with retries and a per-request timeout.

    Never raises; returns a (possibly empty) DataFrame. When retry_on_empty is True, an
    empty result also counts as a failed attempt and is retried; after all attempts the
    (empty) result is returned and logged.
    """
    payload = {
        "name": table_name,
        "user": Unifier.user,
        "token": Unifier.token,
        "back_to": back_to,
        "up_to": up_to,
        "disable_view": False,
    }
    for attempt in range(1, RETRIES + 1):
        try:
            response = requests.post(
                QUERY_URL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and "error" in data:
                raise RuntimeError(f"server error: {data['error']}")
            df = parse_rows(data)
            if df.empty and retry_on_empty:
                if attempt < RETRIES:
                    logger.warning(
                        "Query back_to=%s up_to=%s returned empty (attempt %d/%d), retrying...",
                        back_to,
                        up_to,
                        attempt,
                        RETRIES,
                    )
                    time.sleep(RETRY_WAIT_SECONDS * attempt)
                    continue
                logger.warning(
                    "Query back_to=%s up_to=%s returned empty after %d attempts",
                    back_to,
                    up_to,
                    RETRIES,
                )
            return df
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Query back_to=%s up_to=%s failed (attempt %d/%d): %s (timeout=%ss)",
                back_to,
                up_to,
                attempt,
                RETRIES,
                exc,
                timeout,
            )
            if attempt < RETRIES:
                time.sleep(RETRY_WAIT_SECONDS * attempt)
    return pd.DataFrame()


def get_trading_dates(start_date: date, end_date: date) -> list[str]:
    """Get the list of trading dates in [start_date, end_date] from the NYSE trading calendar."""
    schedule = pmc.get_calendar(CALENDAR).schedule(
        start_date=start_date, end_date=end_date
    )
    trading_dates = [
        pd.Timestamp(ts).strftime("%Y-%m-%d") for ts in schedule.index.to_list()
    ]
    logger.info(
        "Phase 1/2: got %d trading dates from %s calendar (%s..%s)",
        len(trading_dates),
        CALENDAR,
        start_date,
        end_date,
    )
    logger.info("Trading dates (%d): %s", len(trading_dates), ", ".join(trading_dates))
    return trading_dates


def save_partition(df: pd.DataFrame, output_dir: Path, asof_date: str) -> Path:
    df = df.copy()
    if "asof_date" in df.columns:
        df["asof_date"] = df["asof_date"].astype(str)
    else:
        df["asof_date"] = asof_date
    partition_dir = output_dir / f"asof_date={asof_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"part-00000-{asof_date}.parquet"
    df.to_parquet(file_path, engine="pyarrow", index=False)
    return file_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("start_date", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("end_date", help="Inclusive end date, YYYY-MM-DD")
    parser.add_argument(
        "--table",
        required=True,
        help="Unifier table/query name, e.g. lseg_us_eqt_flow_r3k_with_isin_daily_v1_2_trial",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Root output dir; partitions written as <output>/asof_date=YYYY-MM-DD/",
    )
    parser.add_argument("--user", required=True, help="Unifier user name")
    parser.add_argument("--token", required=True, help="Unifier API token")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=(
            "Per-query HTTP timeout in seconds "
            f"(default: {DEFAULT_TIMEOUT}; 1-min tables may need a larger value)"
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-fetch and overwrite existing partitions",
    )
    args = parser.parse_args()

    setup_logging()
    set_credentials(args.user, args.token)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    dates = get_trading_dates(start_date, end_date)
    if not dates:
        logger.error(
            "No trading dates found in %s..%s; aborting.", start_date, end_date
        )
        sys.exit(1)

    logger.info(
        "Phase 2/2: fetching full data day by day (%d trading dates) from table=%s -> %s",
        len(dates),
        args.table,
        output_dir,
    )
    total_rows = 0
    skipped, empty, failed = [], [], []
    started_at = time.time()
    run_started_at = started_at
    for i, day in enumerate(dates, start=1):
        partition_dir = output_dir / f"asof_date={day}"
        if partition_dir.exists() and not args.overwrite:
            logger.info(
                "[%d/%d] %s: partition already exists, skipping", i, len(dates), day
            )
            skipped.append(day)
            continue
        t0 = time.time()
        logger.info(
            "[%d/%d] %s: fetching full-day data (timeout %ss, attempt 1)...",
            i, len(dates), day, args.timeout,
        )
        df = fetch_df(
            args.table,
            day,
            next_day(date.fromisoformat(day)),
            timeout=args.timeout,
            retry_on_empty=True,
        )
        if df.empty:
            logger.warning(
                "[%d/%d] %s: no data after %d attempts, skipping this date",
                i,
                len(dates),
                day,
                RETRIES,
            )
            empty.append(day)
            continue
        try:
            save_partition(df, output_dir, day)
        except Exception as exc:  # noqa: BLE001
            logger.error("[%d/%d] %s: FAILED to save: %s", i, len(dates), day, exc)
            failed.append(day)
            continue
        total_rows += len(df)
        elapsed_total = time.time() - started_at
        eta_seconds = (elapsed_total / i) * (len(dates) - i)
        logger.info(
            "[%d/%d] %s: %d rows in %.1fs | elapsed %s | ETA %s",
            i,
            len(dates),
            day,
            len(df),
            time.time() - t0,
            format_duration(elapsed_total),
            format_duration(eta_seconds),
        )

    elapsed = time.time() - started_at
    logger.info("=" * 60)
    logger.info(
        "DONE in %s | %d/%d dates saved (%d rows) | skipped: %d | empty: %d | failed: %d",
        format_duration(elapsed),
        len(dates) - len(failed) - len(skipped),
        len(dates),
        total_rows,
        len(skipped),
        len(empty),
        len(failed),
    )
    logger.info(
        "Total run time: %s | Output dir: %s",
        format_duration(time.time() - run_started_at),
        output_dir,
    )
    if failed:
        logger.error("Failed dates (retry with --overwrite): %s", ", ".join(failed))
    if empty:
        logger.warning("Dates with no data: %s", ", ".join(empty))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
