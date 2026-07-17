#!/usr/bin/env python3
"""Quant Ops adapter for the vendor stock-info updater."""

import argparse
import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path

VENDOR_ROOT = Path(__file__).resolve().parent
SCRIPTS_ROOT = VENDOR_ROOT.parent
sys.path.insert(0, str(VENDOR_ROOT))
sys.path.insert(0, str(SCRIPTS_ROOT))

import rqdatac

from lib import common_tables
from runtime import init_rqdatac, log_write_mode
from my_utils import trade_date_pre


def load_stock_info_module():
    """Load the migrated entry directly; its package __init__ references files not supplied by the vendor."""
    module_path = VENDOR_ROOT / 'batch_update' / 'basic_data' / 'stock_info.py'
    spec = importlib.util.spec_from_file_location('vendor_stock_info_batch', module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'cannot load stock-info entry: {module_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_date(value: str) -> str:
    normalized = value.replace('-', '').replace('.', '')
    try:
        return datetime.strptime(normalized, '%Y%m%d').strftime('%Y.%m.%d')
    except ValueError as exc:
        raise argparse.ArgumentTypeError('date must use YYYYMMDD or YYYY-MM-DD') from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run vendor stock-info updates.')
    parser.add_argument('--trade-date', type=parse_date)
    parser.add_argument('--start-date', type=parse_date)
    parser.add_argument('--end-date', type=parse_date)
    parser.add_argument('--force', action='store_true')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true', help='Disable database writes (default).')
    mode.add_argument('--enable-write', action='store_true', help='Explicitly enable production database writes.')
    parser.add_argument('--test-write', action='store_true', help='Write output tables only to <name>_test.')
    return parser.parse_args()


def configure(args: argparse.Namespace) -> None:
    if args.trade_date and (args.start_date or args.end_date):
        raise SystemExit('--trade-date cannot be combined with --start-date/--end-date')

    start_date = args.trade_date or args.start_date
    end_date = args.trade_date or args.end_date
    if start_date:
        common_tables.TimeSeriesTable.begin_time = start_date
    if end_date:
        common_tables.TimeSeriesTable.end_time = end_date
    if start_date and end_date and start_date > end_date:
        raise SystemExit('--start-date must not be later than --end-date')

    common_tables.TimeSeriesTable.overwrite = args.force
    common_tables.TimeSeriesTable.force_date_range = bool(args.trade_date or args.force)
    os.environ['ENV_TYPE'] = 'production' if args.enable_write else 'development'
    os.environ['DDB_TEST_WRITE'] = 'true' if args.test_write else 'false'


def main() -> int:
    args = parse_args()
    if not (args.trade_date or args.start_date or args.end_date):
        today = datetime.now().strftime('%Y%m%d')
        if not trade_date_pre.is_trade_date(today):
            print(f"stock_info | skip | {today} is not a trade date")
            return 0
        args.trade_date = parse_date(today)
    configure(args)
    log_write_mode('stock_info')
    init_rqdatac(rqdatac)
    stock_info = load_stock_info_module()
    stock_info.main()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
