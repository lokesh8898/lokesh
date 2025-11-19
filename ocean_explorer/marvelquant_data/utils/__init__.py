"""
Utility Functions for MarvelQuant Data Management.

Provides:
- Contract generators (OptionContract, FuturesContract metadata)
- NSE calendar (holiday and trading day calculations)
- Expiry calculator (monthly/weekly expiry dates, bucket classification)
- Timestamp conversion (IST to UTC nanoseconds)

Usage:
    >>> from marvelquant_data.utils import create_options_contract
    >>> from marvelquant_data.utils import NSEHolidayCalendar
    >>> from marvelquant_data.utils import get_nse_monthly_expiry
    >>> from marvelquant_data.utils import yyyymmdd_seconds_to_utc_ns
"""

from .contract_generators import (
    create_options_contract,
    create_futures_contract,
    parse_nse_option_symbol,
    NSE_LOT_SIZES,
    ASSET_CLASS_MAP,
)

from .nse_calendar import NSEHolidayCalendar

from .expiry_calculator import (
    get_nse_monthly_expiry,
    get_nse_weekly_expiries,
    classify_expiry_bucket,
    get_expiry_bucket_description,
    get_all_monthly_expiries,
    is_expiry_day,
)

from .timestamp_conversion import (
    yyyymmdd_seconds_to_utc_ns,
    validate_timestamp_conversion,
    seconds_since_midnight_to_time_str,
    utc_ns_to_datetime_str,
    analyze_timestamp_field,
    IST_OFFSET,
)

__all__ = [
    # Contract generators
    "create_options_contract",
    "create_futures_contract",
    "parse_nse_option_symbol",
    "NSE_LOT_SIZES",
    "ASSET_CLASS_MAP",
    # Calendar
    "NSEHolidayCalendar",
    # Expiry calculator
    "get_nse_monthly_expiry",
    "get_nse_weekly_expiries",
    "classify_expiry_bucket",
    "get_expiry_bucket_description",
    "get_all_monthly_expiries",
    "is_expiry_day",
    # Timestamp conversion
    "yyyymmdd_seconds_to_utc_ns",
    "validate_timestamp_conversion",
    "seconds_since_midnight_to_time_str",
    "utc_ns_to_datetime_str",
    "analyze_timestamp_field",
    "IST_OFFSET",
]
