#!/usr/bin/env python3
"""Compare all rows and columns for one date between two DolphinDB tables."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import dolphindb as ddb
import numpy as np
import pandas as pd


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fully compare formal and test DolphinDB tables for one date."
    )
    parser.add_argument("--trade-date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--database", default="market_data_1d")
    parser.add_argument("--formal-table", default="index")
    parser.add_argument("--test-table", default="index_test")
    parser.add_argument("--key-columns", nargs="+", default=None)
    parser.add_argument("--time-column", choices=["date", "datetime"], default=None)
    parser.add_argument("--atol", type=float, default=0.0)
    parser.add_argument("--rtol", type=float, default=0.0)
    parser.add_argument(
        "--output-dir",
        default=str(
            Path(__file__).resolve().parents[3]
            / "comparison_reports"
        ),
    )
    parser.add_argument(
        "--fail-on-difference",
        action="store_true",
        help="Return exit code 1 when any schema, key, or value difference exists.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not re.fullmatch(r"\d{8}", args.trade_date):
        raise ValueError("--trade-date must use YYYYMMDD format")
    identifiers = [args.database, args.formal_table, args.test_table]
    identifiers.extend(args.key_columns or [])
    if args.time_column:
        identifiers.append(args.time_column)
    for value in identifiers:
        if not IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid DolphinDB identifier: {value}")
    if args.atol < 0 or args.rtol < 0:
        raise ValueError("--atol and --rtol must be non-negative")


def connect() -> ddb.session:
    session = ddb.session()
    session.connect(
        os.getenv("DOLPHINDB_HOST", "${DOLPHINDB_HOST}"),
        int(os.getenv("DOLPHINDB_PORT", "${DOLPHINDB_PORT}")),
        os.getenv("DB_USERNAME", "your_username"),
        os.getenv("DB_PASSWORD", "your_password"),
    )
    return session


def load_date(
    session: ddb.session,
    database: str,
    table: str,
    trade_date: str,
    time_column: str,
) -> pd.DataFrame:
    if not session.run(f'existsTable("dfs://{database}", "{table}")'):
        raise RuntimeError(f"DolphinDB table does not exist: dfs://{database}/{table}")
    date_literal = f"{trade_date[:4]}.{trade_date[4:6]}.{trade_date[6:8]}"
    date_expression = f"date({time_column})" if time_column == "datetime" else time_column
    result = session.run(
        f'select * from loadTable("dfs://{database}", "{table}") '
        f"where {date_expression} = {date_literal}"
    )
    if not isinstance(result, pd.DataFrame):
        return pd.DataFrame(result)
    return result


def json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return str(pd.Timestamp(value))
    if isinstance(value, np.generic):
        return value.item()
    return value


def equal_mask(
    formal: pd.Series,
    test: pd.Series,
    atol: float,
    rtol: float,
) -> np.ndarray:
    both_null = formal.isna().to_numpy() & test.isna().to_numpy()
    if pd.api.types.is_numeric_dtype(formal) and pd.api.types.is_numeric_dtype(test):
        left = pd.to_numeric(formal, errors="coerce").to_numpy(dtype=float)
        right = pd.to_numeric(test, errors="coerce").to_numpy(dtype=float)
        return np.isclose(left, right, atol=atol, rtol=rtol, equal_nan=True)
    return both_null | (
        formal.astype("string").fillna("<NULL>").to_numpy()
        == test.astype("string").fillna("<NULL>").to_numpy()
    )


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def compare(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    session = connect()
    try:
        formal_schema = session.run(
            f'schema(loadTable("dfs://{args.database}", "{args.formal_table}"))'
        )
        formal_columns = formal_schema["colDefs"]["name"].astype(str).tolist()
        time_column = args.time_column
        if time_column is None:
            time_column = "date" if "date" in formal_columns else "datetime"
        key_columns = args.key_columns or ["code", time_column]
        args.time_column = time_column
        args.key_columns = key_columns
        formal = load_date(
            session, args.database, args.formal_table, args.trade_date, time_column
        )
        test = load_date(
            session, args.database, args.test_table, args.trade_date, time_column
        )
    finally:
        session.close()

    report_dir = (
        Path(args.output_dir)
        / args.database
        / args.trade_date
        / f"{args.formal_table}_vs_{args.test_table}"
    )
    report_dir.mkdir(parents=True, exist_ok=True)

    missing_keys_formal = [c for c in args.key_columns if c not in formal.columns]
    missing_keys_test = [c for c in args.key_columns if c not in test.columns]
    if missing_keys_formal or missing_keys_test:
        raise RuntimeError(
            "Missing key columns: "
            f"formal={missing_keys_formal}, test={missing_keys_test}"
        )

    duplicate_formal = formal[
        formal.duplicated(args.key_columns, keep=False)
    ].sort_values(args.key_columns)
    duplicate_test = test[
        test.duplicated(args.key_columns, keep=False)
    ].sort_values(args.key_columns)
    write_csv(duplicate_formal, report_dir / "duplicate_keys_formal.csv")
    write_csv(duplicate_test, report_dir / "duplicate_keys_test.csv")

    formal_columns = list(formal.columns)
    test_columns = list(test.columns)
    only_columns_formal = [c for c in formal_columns if c not in test_columns]
    only_columns_test = [c for c in test_columns if c not in formal_columns]
    common_columns = [
        c for c in formal_columns if c in test_columns and c not in args.key_columns
    ]

    formal_unique = formal.drop_duplicates(args.key_columns, keep="last")
    test_unique = test.drop_duplicates(args.key_columns, keep="last")
    merged = formal_unique.merge(
        test_unique,
        on=args.key_columns,
        how="outer",
        suffixes=("_formal", "_test"),
        indicator=True,
        validate="one_to_one",
    )

    only_formal = merged.loc[merged["_merge"] == "left_only"].copy()
    only_test = merged.loc[merged["_merge"] == "right_only"].copy()
    common = merged.loc[merged["_merge"] == "both"].copy()
    write_csv(only_formal, report_dir / "only_in_formal.csv")
    write_csv(only_test, report_dir / "only_in_test.csv")

    value_differences: list[dict[str, Any]] = []
    differing_rows = np.zeros(len(common), dtype=bool)
    column_difference_counts: dict[str, int] = {}

    for column in common_columns:
        formal_values = common[f"{column}_formal"]
        test_values = common[f"{column}_test"]
        different = ~equal_mask(
            formal_values, test_values, args.atol, args.rtol
        )
        count = int(different.sum())
        column_difference_counts[column] = count
        differing_rows |= different
        if not count:
            continue
        positions = np.flatnonzero(different)
        for position in positions:
            row = common.iloc[position]
            record = {
                key: json_value(row[key]) for key in args.key_columns
            }
            record.update(
                {
                    "column": column,
                    "formal_value": json_value(row[f"{column}_formal"]),
                    "test_value": json_value(row[f"{column}_test"]),
                }
            )
            value_differences.append(record)

    differences_frame = pd.DataFrame(
        value_differences,
        columns=[
            *args.key_columns,
            "column",
            "formal_value",
            "test_value",
        ],
    )
    write_csv(differences_frame, report_dir / "value_differences.csv")

    summary: dict[str, Any] = {

    }
    summary["identical"] = not any(
        [
            only_columns_formal,
            only_columns_test,
            len(duplicate_formal),
            len(duplicate_test),
            len(only_formal),
            len(only_test),
            len(differences_frame),
        ]
    )
    (report_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary, report_dir


def print_summary(summary: dict[str, Any], report_dir: Path) -> None:
    print(
        f"compare | {summary['database']} | "
        f"{summary['formal_table']} vs {summary['test_table']} | "
        f"date={summary['trade_date']}"
    )
    print(
        "rows | "
        f"formal={summary['formal_rows']}, test={summary['test_rows']}, "
        f"common={summary['common_keys']}, "
        f"only_formal={summary['only_in_formal']}, "
        f"only_test={summary['only_in_test']}"
    )
    print(
        "values | "
        f"differing_rows={summary['differing_common_rows']}, "
        f"differing_cells={summary['differing_cells']}"
    )
    nonzero_columns = {
        column: count
        for column, count in summary["column_difference_counts"].items()
        if count
    }
    print(f"column_differences | {nonzero_columns}")
    print(f"identical | {summary['identical']}")
    print(f"report_dir | {report_dir}")


def main() -> int:
    args = parse_args()
    validate_args(args)
    summary, report_dir = compare(args)
    print_summary(summary, report_dir)
    if args.fail_on_difference and not summary["identical"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
