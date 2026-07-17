import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse


SCRIPTS_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_ROOT.parents[1]


def is_production() -> bool:
    return os.environ.get('ENV_TYPE', 'development').lower() == 'production'


def log_write_mode(task_name: str) -> None:
    if is_production():
        print(f'{task_name} | mode | production write enabled')
        return
    print(f'{task_name} | mode | development dry-run, database writes disabled')


def get_data_root() -> Path:
    configured = os.environ.get('OPS_DATA_ROOT')
    data_root = Path(configured) if configured else PROJECT_ROOT / 'data'
    data_root.mkdir(parents=True, exist_ok=True)
    return data_root


def resolve_trade_dates(default_offset_days: int = 1) -> list[str]:
    raw_dates = os.environ.get('OPS_TRADE_DATES', '').strip()
    if raw_dates:
        return [item.strip() for item in raw_dates.split(',') if item.strip()]

    single_date = os.environ.get('OPS_TRADE_DATE', '').strip()
    if single_date:
        return [single_date]

    default_date = (datetime.now() - timedelta(days=default_offset_days)).strftime('%Y%m%d')
    return [default_date]


def should_force_download() -> bool:
    return os.environ.get('OPS_FORCE_DOWNLOAD', 'true').strip().lower() not in {'0', 'false', 'no'}


DEFAULT_RQDATAC_LICENSE = os.getenv("RQ_LICENSE_KEY", "your_rq_license_key")


def init_rqdatac(rqdatac_module) -> None:
    """Initialize rqdatac from environment first, then fallback to the bundled RQData license (${RQ_LICENSE_KEY})."""
    if os.environ.get('RQDATAC2_CONF') or os.environ.get('RQDATAC_CONF'):
        rqdatac_module.init()
        return

    license_value = os.environ.get('RQDATAC_LICENSE', '').strip() or DEFAULT_RQDATAC_LICENSE
    if license_value:
        rqdatac_module.init(os.getenv("RQ_USERNAME", "your_username"), os.getenv("RQ_PASSWORD", "your_password"), os.getenv("RQ_LICENSE_KEY", "your_rq_license_key"))
        return

    username = os.environ.get('RQDATAC_USERNAME', '').strip()
    password = os.environ.get('RQDATAC_PASSWORD', '').strip()
    address = os.environ.get('RQDATAC_ADDR', '').strip()
    if username and password:
        if address:
            parsed = urlparse(address if '://' in address else f'tcp://{address}')
            if not parsed.hostname or not parsed.port:
                raise RuntimeError('RQDATAC_ADDR must use host:port')
            rqdatac_module.init(username, password, (parsed.hostname, parsed.port))
        else:
            rqdatac_module.init(username, password)
        return

    raise RuntimeError(
        'RQData credentials are missing. Set RQDATAC_CONF/RQDATAC2_CONF, '
        'RQDATAC_LICENSE, or RQDATAC_USERNAME and RQDATAC_PASSWORD.'
    )
