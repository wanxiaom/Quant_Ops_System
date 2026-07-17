#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Count configured DolphinDB tables and write daily snapshots to MySQL."""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import date as date_cls
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import dolphindb as ddb
import pymysql


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DB_PATH_RE = re.compile(r"^dfs://[A-Za-z0-9_./-]+$")
FORBIDDEN_WHERE_RE = re.compile(
    r"(;|--|/\*|\*/|\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b)",
    re.IGNORECASE,
)
EXCLUDED_DATABASE_GROUPS = {"quick_ic", "operator_ic"}
GROUP_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
DDB_DATABASE_CATEGORY = {
  
}


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    return int(value)


def mysql_conn():
    return pymysql.connect(
        host=os.environ.get("DB_HOST", "-"),
        port=env_int("DB_PORT", -),
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASS") or os.environ.get("DB_PASSWORD", "ops_password"),
        database=os.environ.get("DB_NAME", "quant_ops"),
        charset="utf8mb4",
        autocommit=True,
        init_command="SET time_zone = '+08:00'",
        cursorclass=pymysql.cursors.DictCursor,
    )


def ddb_session():
    host = os.environ.get("DOLPHINDB_HOST", "${DOLPHINDB_HOST}")
    port = int(os.environ.get("DOLPHINDB_PORT", "${DOLPHINDB_PORT}"))
    user = os.environ.get("DB_USERNAME", "your_username")
    password = os.environ.get("DB_PASSWORD", "your_password")
    sess = ddb.session()
    sess.connect(host, port, user, password)
    return sess


def normalize_date(value: Optional[str]) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    raise ValueError(f"invalid date: {value}, expected YYYY-MM-DD or YYYYMMDD")


def default_target_date(conn) -> str:
    yesterday = (date_cls.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(trade_date) AS d FROM trade_calendar WHERE trade_date <= %s", (yesterday,))
        row = cur.fetchone()
    return str(row["d"]) if row and row.get("d") else yesterday


def trade_dates_between(conn, start_date: str, end_date: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date FROM trade_calendar WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (start_date, end_date),
        )
        rows = cur.fetchall()
    dates = [str(row["trade_date"]) for row in rows]
    if dates:
        return dates

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        raise ValueError("start_date must be <= end_date")
    result: List[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            result.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return result


def normalize_frequency(value: Optional[str]) -> str:
    text = (value or "daily").strip().lower()
    if text in {"weekly", "week", "w", "1w", "周频"}:
        return "weekly"
    if text in {"monthly", "month", "m", "1m", "月频"}:
        return "monthly"
    return "daily"


def last_trade_date_of_week(conn, target_date: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(trade_date) AS d
            FROM trade_calendar
            WHERE YEARWEEK(STR_TO_DATE(trade_date, '%%Y-%%m-%%d'), 1) =
                  YEARWEEK(STR_TO_DATE(%s, '%%Y-%%m-%%d'), 1)
            """,
            (target_date,),
        )
        row = cur.fetchone()
    return str(row["d"]) if row and row.get("d") else None


def last_trade_date_of_month(conn, target_date: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(trade_date) AS d
            FROM trade_calendar
            WHERE DATE_FORMAT(STR_TO_DATE(trade_date, '%%Y-%%m-%%d'), '%%Y-%%m') =
                  DATE_FORMAT(STR_TO_DATE(%s, '%%Y-%%m-%%d'), '%%Y-%%m')
            """,
            (target_date,),
        )
        row = cur.fetchone()
    return str(row["d"]) if row and row.get("d") else None


def is_last_weekday_of_month(target_date: str) -> bool:
    current = datetime.strptime(target_date, "%Y-%m-%d").date()
    probe = current + timedelta(days=1)
    while probe.month == current.month:
        if probe.weekday() < 5:
            return False
        probe += timedelta(days=1)
    return True


def is_expected_data_day(conn, target_date: str, frequency: str) -> bool:
    normalized = normalize_frequency(frequency)
    if normalized == "daily":
        return True
    if normalized == "weekly":
        last = last_trade_date_of_week(conn, target_date)
        if last:
            return last == target_date
        return datetime.strptime(target_date, "%Y-%m-%d").date().weekday() == 4
    if normalized == "monthly":
        last = last_trade_date_of_month(conn, target_date)
        if last:
            return last == target_date
        return is_last_weekday_of_month(target_date)
    return True


def resolve_target_dates(conn, args: argparse.Namespace) -> List[str]:
    if args.date and (args.start_date or args.end_date):
        raise ValueError("--date cannot be used together with --start-date/--end-date")
    if args.date:
        return [normalize_date(args.date)]
    if args.start_date or args.end_date:
        start = normalize_date(args.start_date or args.end_date)
        end = normalize_date(args.end_date or args.start_date)
        return trade_dates_between(conn, start, end)
    return [default_target_date(conn)]



def normalize_database_path(value: str) -> str:
    text = str(value).strip()
    return text if text.startswith("dfs://") else f"dfs://{text}"


def database_group_name(database: str) -> str:
    return database.replace("dfs://", "", 1).strip("/")


def ddb_category_name(database: str) -> str:
    name = database_group_name(normalize_database_path(database))
    return DDB_DATABASE_CATEGORY.get(name, name).lower()


def ensure_monitor_group(conn, group_id: str, display_name: Optional[str] = None) -> None:
    if not GROUP_ID_RE.match(group_id):
        raise ValueError(f"invalid group_id: {group_id}")
    sql = """
        INSERT IGNORE INTO ddb_monitor_group
            (group_id, display_name, sort_order, enabled, description, created_at, updated_at)
        VALUES (%s, %s, 100, 1, 'auto-discovered from DolphinDB', NOW(), NOW())
    """
    with conn.cursor() as cur:
        cur.execute(sql, (group_id, display_name or group_id))


def is_excluded_database(database: str) -> bool:
    return database_group_name(normalize_database_path(database)) in EXCLUDED_DATABASE_GROUPS


def monitor_id_for(database: str, table_name: str) -> str:
    raw = f"mon_{database_group_name(database)}_{table_name}"
    return re.sub(r"[^A-Za-z0-9_]+", "_", raw).strip("_")[:64]


def to_str_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    try:
        return [str(item) for item in list(value)]
    except TypeError:
        return [str(value)]


def discover_databases(sess, only_databases: List[str]) -> List[str]:
    if only_databases:
        return [
            database
            for database in (normalize_database_path(item) for item in only_databases)
            if not is_excluded_database(database)
        ]
    databases = to_str_list(sess.run("getClusterDFSDatabases()"))
    return sorted(
        db
        for db in databases
        if db.startswith("dfs://") and not is_excluded_database(db)
    )


def discover_tables(sess, database: str) -> List[str]:
    tables = to_str_list(sess.run(f"getTables(database({ddb_string(database)}))"))
    return sorted(table for table in tables if IDENT_RE.match(table))


def table_columns(sess, database: str, table_name: str) -> List[Dict[str, str]]:
    schema = sess.run(f"schema(loadTable({ddb_string(database)}, {ddb_string(table_name)}))")
    col_defs = schema.get("colDefs") if isinstance(schema, dict) else None
    if col_defs is None:
        return []
    records = col_defs.to_dict("records") if hasattr(col_defs, "to_dict") else list(col_defs)
    result: List[Dict[str, str]] = []
    for row in records:
        name = str(row.get("name", ""))
        type_string = str(row.get("typeString", ""))
        if name:
            result.append({"name": name, "type": type_string.upper()})
    return result


def infer_date_column(columns: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    by_name = {col["name"].lower(): col for col in columns}
    for name in ("date", "trade_date", "report_date", "update_date", "end_date", "datetime", "time"):
        if name in by_name:
            return by_name[name]
    for col in columns:
        lower = col["name"].lower()
        type_string = col["type"]
        if "date" in lower or type_string in {"DATE", "DATETIME", "TIMESTAMP", "MONTH"}:
            return col
    return None


def date_format_for_column(column: Dict[str, str]) -> str:
    type_string = column.get("type", "").upper()
    if type_string in {"STRING", "SYMBOL"}:
        return "YYYY-MM-DD_STR"
    if type_string in {"INT", "LONG", "SHORT"}:
        return "YYYYMMDD_INT"
    if type_string in {"DATETIME", "TIMESTAMP"}:
        return "DATETIME"
    return "YYYY-MM-DD"


def upsert_monitor(conn, monitor: Dict[str, str], enabled: int) -> None:
    ensure_monitor_group(conn, monitor["group_id"], monitor.get("group_display_name") or monitor["group_id"])
    sql = """
        INSERT INTO ddb_monitor
            (monitor_id, `database`, table_name, date_column, date_format, where_extra, group_name, group_id,
             related_task_ids, enabled, description, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, '', %s, %s, CAST('[]' AS JSON), %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            `database` = VALUES(`database`),
            table_name = VALUES(table_name),
            date_column = VALUES(date_column),
            date_format = VALUES(date_format),
            group_id = VALUES(group_id),
            description = VALUES(description),
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                monitor["monitor_id"],
                monitor["database"],
                monitor["table_name"],
                monitor["date_column"],
                monitor["date_format"],
                monitor["group_id"],
                monitor["group_id"],
                enabled,
                monitor["description"],
            ),
        )


def delete_monitors_for_database(
    conn, database: str, current_tables: Optional[Iterable[str]] = None
) -> int:
    """Remove monitor rows (and snapshots) for tables absent from DolphinDB."""
    with conn.cursor() as cur:
        params: List[Any] = [database]
        if current_tables:
            tables = sorted(set(current_tables))
            placeholders = ", ".join(["%s"] * len(tables))
            sql = (
                "SELECT monitor_id FROM ddb_monitor "
                f"WHERE `database` = %s AND table_name NOT IN ({placeholders})"
            )
            params.extend(tables)
        else:
            sql = "SELECT monitor_id FROM ddb_monitor WHERE `database` = %s"
        cur.execute(sql, params)
        monitor_ids = [row["monitor_id"] for row in cur.fetchall()]
        if not monitor_ids:
            return 0

        placeholders = ", ".join(["%s"] * len(monitor_ids))
        cur.execute(
            f"DELETE FROM ddb_monitor_snapshot WHERE monitor_id IN ({placeholders})",
            monitor_ids,
        )
        cur.execute(
            f"DELETE FROM ddb_monitor WHERE monitor_id IN ({placeholders})",
            monitor_ids,
        )
        return len(monitor_ids)


def sync_monitors_from_dolphindb(conn, sess, databases: List[str], replace: bool = False) -> int:
    if replace:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ddb_monitor_snapshot")
            cur.execute("DELETE FROM ddb_monitor")

    discovered_databases = discover_databases(sess, databases)
    requested_databases = {
        normalize_database_path(database)
        for database in databases
        if not is_excluded_database(database)
    }
    existing_databases: set[str] = set()
    if not databases and not replace:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT `database` FROM ddb_monitor "
                "WHERE `database` NOT IN (%s, %s)",
                tuple(normalize_database_path(group) for group in sorted(EXCLUDED_DATABASE_GROUPS)),
            )
            existing_databases = {str(row["database"]) for row in cur.fetchall()}

    discovered = 0
    skipped = 0
    successful_databases: set[str] = set()
    for database in discovered_databases:
        group_id = ddb_category_name(database)
        try:
            tables = discover_tables(sess, database)
        except Exception as exc:
            skipped += 1
            print(f"sync | {database} | failed to list tables | {exc}", file=sys.stderr)
            continue
        successful_databases.add(database)
        if not replace:
            removed = delete_monitors_for_database(conn, database, tables)
            if removed:
                print(f"sync | {database} | removed stale monitors={removed}")
        for table_name in tables:
            try:
                columns = table_columns(sess, database, table_name)
                date_col = infer_date_column(columns)
                if not date_col:
                    skipped += 1
                    print(f"sync | {database}/{table_name} | skipped | no date-like column", file=sys.stderr)
                    continue
                monitor = {
                    "monitor_id": monitor_id_for(database, table_name),
                    "database": database,
                    "table_name": table_name,
                    "date_column": date_col["name"],
                    "date_format": date_format_for_column(date_col),
                    "group_id": group_id,
                    "group_display_name": group_id,
                    "description": "auto-discovered from DolphinDB",
                }
                upsert_monitor(conn, monitor, 1)
                discovered += 1
                print(f"sync | {database}/{table_name} | date_column={date_col['name']} | group={group_id}")
            except Exception as exc:
                skipped += 1
                print(f"sync | {database}/{table_name} | failed | {exc}", file=sys.stderr)

    if not databases and not replace:
        discovered_set = set(discovered_databases)
        for database in sorted(existing_databases - discovered_set):
            removed = delete_monitors_for_database(conn, database)
            if removed:
                print(f"sync | {database} | database missing | removed stale monitors={removed}")
    elif databases and not replace:
        for database in sorted(requested_databases - successful_databases):
            # A failed table listing is not treated as deletion; this avoids data loss on transient errors.
            print(f"sync | {database} | not cleaned because table discovery failed", file=sys.stderr)

    print(f"sync summary | monitors={discovered} skipped={skipped}")
    return discovered

def load_monitors(conn, table: str = "", database: str = "", monitor_id: str = "") -> List[Dict[str, Any]]:
    clauses = ["m.enabled = 1", "g.enabled = 1"]
    params: List[Any] = []
    for excluded_group in sorted(EXCLUDED_DATABASE_GROUPS):
        clauses.append("`database` <> %s")
        params.append(normalize_database_path(excluded_group))
    if monitor_id:
        clauses.append("monitor_id = %s")
        params.append(monitor_id)
    if table:
        clauses.append("(table_name = %s OR monitor_id = %s)")
        params.extend([table, table])
    if database:
        clauses.append("`database` = %s")
        params.append(database)
    sql = (
        "SELECT monitor_id, `database`, table_name, date_column, date_format, COALESCE(frequency, 'daily') AS frequency, where_extra, "
        "m.group_id FROM ddb_monitor m "
        "INNER JOIN ddb_monitor_group g ON g.group_id = m.group_id WHERE "
        + " AND ".join(clauses)
        + " ORDER BY m.group_id, m.`database`, m.table_name"
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def validate_monitor(monitor: Dict[str, Any]) -> None:
    database = monitor["database"]
    table = monitor["table_name"]
    date_column = monitor["date_column"]
    where_extra = (monitor.get("where_extra") or "").strip()
    if not DB_PATH_RE.match(database):
        raise ValueError(f"invalid DolphinDB database path: {database}")
    if not IDENT_RE.match(table):
        raise ValueError(f"invalid table name: {table}")
    if not IDENT_RE.match(date_column):
        raise ValueError(f"invalid date column: {date_column}")
    if where_extra and FORBIDDEN_WHERE_RE.search(where_extra):
        raise ValueError(f"unsafe where_extra for {database}/{table}")


def ddb_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def ddb_date_literal(target_date: str, date_format: str) -> str:
    compact = target_date.replace("-", "")
    fmt = (date_format or "YYYY-MM-DD").upper()
    if fmt in ("YYYYMMDD_INT", "INT"):
        return compact
    if fmt in ("YYYYMMDD_STR", "STRING_YYYYMMDD"):
        return ddb_string(compact)
    if fmt in ("YYYY-MM-DD_STR", "STRING_YYYY-MM-DD"):
        return ddb_string(target_date)
    if fmt in ("YYYYMMDD", "YYYY-MM-DD"):
        value = compact if fmt == "YYYYMMDD" else target_date
        pattern = "yyyyMMdd" if fmt == "YYYYMMDD" else "yyyy-MM-dd"
        return f'temporalParse("{value}", "{pattern}")'
    return f'temporalParse("{target_date}", "yyyy-MM-dd")'


def ddb_datetime_range(target_date: str) -> tuple[str, str]:
    """Return a half-open [day start, next day start) DolphinDB datetime range."""
    start = datetime.strptime(target_date, "%Y-%m-%d")
    end = start + timedelta(days=1)

    def literal(value: datetime) -> str:
        text = value.strftime("%Y-%m-%d %H:%M:%S")
        return f'temporalParse("{text}", "yyyy-MM-dd HH:mm:ss")'

    return literal(start), literal(end)


def count_monitor(sess, monitor: Dict[str, Any], target_date: str) -> int:
    validate_monitor(monitor)
    database = monitor["database"]
    table = monitor["table_name"]
    date_column = monitor["date_column"]
    where_extra = (monitor.get("where_extra") or "").strip()

    exists_script = f"existsTable({ddb_string(database)}, {ddb_string(table)})"
    if not bool(sess.run(exists_script)):
        raise RuntimeError(f"DolphinDB table does not exist: {database}/{table}")

    date_format = (monitor.get("date_format") or "YYYY-MM-DD").upper()
    is_datetime = date_format in {"DATETIME", "TIMESTAMP"} or date_column.lower() in {
        "datetime",
        "timestamp",
        "time",
    }
    if is_datetime:
        start_literal, end_literal = ddb_datetime_range(target_date)
        where = f"{date_column} >= {start_literal} and {date_column} < {end_literal}"
    else:
        literal = ddb_date_literal(target_date, date_format)
        where = f"{date_column} = {literal}"
    if where_extra:
        where += f" and ({where_extra})"
    script = (
        f"t = loadTable({ddb_string(database)}, {ddb_string(table)});"
        f"exec count(*) from t where {where}"
    )
    result = sess.run(script)
    if isinstance(result, (list, tuple)):
        result = result[0] if result else 0
    return int(result)


def upsert_snapshot(
    conn,
    monitor: Dict[str, Any],
    target_date: str,
    row_count: int,
    status: str,
    duration_ms: int,
    error_message: str = "",
) -> None:
    sql = """
        INSERT INTO ddb_monitor_snapshot
            (monitor_id, `database`, table_name, group_name, group_id, `date`, row_count, status, checked_at, duration_ms, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON DUPLICATE KEY UPDATE
            `database` = VALUES(`database`),
            table_name = VALUES(table_name),
            group_name = VALUES(group_name),
            group_id = VALUES(group_id),
            row_count = VALUES(row_count),
            status = VALUES(status),
            checked_at = VALUES(checked_at),
            duration_ms = VALUES(duration_ms),
            error_message = VALUES(error_message)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                monitor["monitor_id"],
                monitor["database"],
                monitor["table_name"],
                monitor["group_id"],
                monitor["group_id"],
                target_date,
                row_count,
                status,
                duration_ms,
                error_message[:4000],
            ),
        )


def run_counts(args: argparse.Namespace) -> int:
    conn = mysql_conn()
    try:
        sync_sess = None
        if args.sync_monitors or args.sync_monitors_only:
            try:
                sync_sess = ddb_session()
                sync_monitors_from_dolphindb(conn, sync_sess, args.sync_database or [], args.replace_monitors)
            finally:
                if sync_sess is not None:
                    try:
                        sync_sess.close()
                    except Exception:
                        pass

        if args.sync_monitors_only:
            return 0

        target_dates = resolve_target_dates(conn, args)

        monitors = load_monitors(
            conn,
            table=args.table or "",
            database=args.database or "",
            monitor_id=args.monitor_id or "",
        )
        if not monitors:
            print("no enabled ddb_monitor rows matched the request", file=sys.stderr)
            return 2

        print(f"ddb_monitor_count | dates={','.join(target_dates)} | monitors={len(monitors)}")
        try:
            sess = ddb_session()
        except Exception as exc:
            message = f"DolphinDB connection failed: {exc}"
            failed = 0
            for target_date in target_dates:
                for monitor in monitors:
                    upsert_snapshot(conn, monitor, target_date, -1, "failed", 0, message)
                    failed += 1
                    print(
                        f"{monitor['database']}/{monitor['table_name']} | {target_date} | failed | {message}",
                        file=sys.stderr,
                    )
            print(f"summary | success=0 zero=0 failed={failed}")
            return 0

        try:
            ok = zero = failed = 0
            for target_date in target_dates:
                for monitor in monitors:
                    started = time.time()
                    label = f"{monitor['database']}/{monitor['table_name']}[{monitor['date_column']}]"
                    try:
                        count = count_monitor(sess, monitor, target_date)
                        expected_data_day = is_expected_data_day(conn, target_date, monitor.get("frequency", "daily"))
                        status = "success" if count > 0 or not expected_data_day else "zero"
                        message = "" if count > 0 or expected_data_day else "non-production trading day for configured frequency"
                        duration_ms = int((time.time() - started) * 1000)
                        upsert_snapshot(conn, monitor, target_date, count, status, duration_ms, message)
                        if status == "success":
                            ok += 1
                        else:
                            zero += 1
                        print(f"{label} | {target_date} | {status} | rows={count} | {duration_ms}ms")
                    except Exception as exc:
                        duration_ms = int((time.time() - started) * 1000)
                        upsert_snapshot(conn, monitor, target_date, -1, "failed", duration_ms, str(exc))
                        failed += 1
                        print(f"{label} | {target_date} | failed | {exc}", file=sys.stderr)
            print(f"summary | success={ok} zero={zero} failed={failed}")
            return 0
        finally:
            try:
                sess.close()
            except Exception:
                pass
    finally:
        conn.close()


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Count configured DolphinDB tables and update MySQL snapshots.")
    parser.add_argument("--date", default="", help="Target date, YYYY-MM-DD or YYYYMMDD. Defaults to latest trade day <= yesterday.")
    parser.add_argument("--trade-date", dest="date", help=argparse.SUPPRESS)
    parser.add_argument("--start-date", default="", help="Start date for batch recount, YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="End date for batch recount, YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--table", default="", help="Only recount one table name or monitor_id.")
    parser.add_argument("--database", default="", help="Optional DolphinDB database path, e.g. dfs://market.")
    parser.add_argument("--monitor-id", default="", help="Only recount one monitor_id.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--sync-monitors", action="store_true", help="Sync DolphinDB tables into ddb_monitor before counting.")
    mode.add_argument("--sync-monitors-only", action="store_true", help="Only sync DolphinDB tables into ddb_monitor; do not count.")
    parser.add_argument("--replace-monitors", action="store_true", help="Clear existing monitor config and snapshots before sync.")
    parser.add_argument("--sync-database", action="append", default=[], help="Only sync one DolphinDB database; can be repeated, e.g. dfs://market_data_1d.")
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    try:
        return run_counts(parse_args(argv))
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
