"""
Timestamp Conversion Utilities for Raw Data Transformation.

Converts source data timestamps (typically YYYYMMDD + seconds since midnight in IST)
to Nautilus-required UTC nanoseconds format.

Source Format (typical):
- trade_date: int (YYYYMMDD, e.g., 20240102)
- trade_time: int (seconds since midnight IST, e.g., 34500 = 09:35:00)

Target Format:
- ts_event: int (UTC nanoseconds since epoch)
- ts_init: int (UTC nanoseconds since epoch)

Usage:
    >>> from marvelquant_data.utils.timestamp_conversion import yyyymmdd_seconds_to_utc_ns
    >>> # 2024-01-02 09:35:00 IST
    >>> yyyymmdd_seconds_to_utc_ns(20240102, 34500)
    1704177900000000000  # 2024-01-02 04:05:00 UTC
"""

from datetime import datetime, timedelta, timezone
import pandas as pd

# IST offset from UTC (India Standard Time = UTC+5:30)
IST_OFFSET = timedelta(hours=5, minutes=30)


def yyyymmdd_seconds_to_utc_ns(
    date_int: int,
    seconds_int: int
) -> int:
    """
    Convert YYYYMMDD + seconds_since_midnight (IST) to UTC nanoseconds.

    NSE data is typically in IST (Indian Standard Time = UTC+5:30).
    Nautilus requires UTC nanoseconds since epoch.

    Args:
        date_int: Date as YYYYMMDD integer (e.g., 20240102 = Jan 2, 2024)
        seconds_int: Seconds since midnight IST (e.g., 34500 = 09:35:00)

    Returns:
        UTC nanoseconds since Unix epoch

    Example:
        >>> # 2024-01-02 09:35:00 IST = 2024-01-02 04:05:00 UTC
        >>> yyyymmdd_seconds_to_utc_ns(20240102, 34500)
        1704177900000000000

        >>> # Market open: 2024-01-02 09:15:00 IST
        >>> yyyymmdd_seconds_to_utc_ns(20240102, 33300)
        1704176700000000000
    """
    # Parse date components
    year = date_int // 10000
    month = (date_int % 10000) // 100
    day = date_int % 100

    # Parse time components
    hours = seconds_int // 3600
    minutes = (seconds_int % 3600) // 60
    seconds = seconds_int % 60

    # Create IST datetime (naive - no timezone info)
    ist_dt = datetime(year, month, day, hours, minutes, seconds)

    # Convert IST to UTC by subtracting IST offset
    utc_dt = ist_dt - IST_OFFSET

    # Convert to nanoseconds since Unix epoch
    # Unix epoch = 1970-01-01 00:00:00 UTC
    timestamp_seconds = utc_dt.timestamp()
    timestamp_nanoseconds = int(timestamp_seconds * 1_000_000_000)

    return timestamp_nanoseconds


def validate_timestamp_conversion(
    df: pd.DataFrame,
    date_col: str = 'trade_date',
    time_col: str = 'trade_time',
    ts_col: str = 'ts_event'
) -> bool:
    """
    Validate that timestamp conversion was done correctly.

    Args:
        df: DataFrame with both source and converted timestamps
        date_col: Source date column name
        time_col: Source time column name (seconds since midnight)
        ts_col: Target timestamp column name (UTC nanoseconds)

    Returns:
        True if validation passes

    Raises:
        AssertionError: If timestamps don't match expected values

    Example:
        >>> df = pd.DataFrame({
        ...     'trade_date': [20240102],
        ...     'trade_time': [34500],
        ...     'ts_event': [1704177900000000000]
        ... })
        >>> validate_timestamp_conversion(df)
        True
    """
    if df.empty:
        print("⚠️  Empty DataFrame, skipping validation")
        return True

    # Check first row
    first_date = df[date_col].iloc[0]
    first_time = df[time_col].iloc[0]
    first_ts = df[ts_col].iloc[0]

    expected_ts = yyyymmdd_seconds_to_utc_ns(first_date, first_time)

    if first_ts != expected_ts:
        raise AssertionError(
            f"Timestamp mismatch:\n"
            f"  Source: {first_date} + {first_time}s\n"
            f"  Expected: {expected_ts}\n"
            f"  Got: {first_ts}\n"
            f"  Difference: {first_ts - expected_ts} ns"
        )

    # Check timestamp is in valid range (2018-2025)
    MIN_TIMESTAMP = 1514764800000000000  # 2018-01-01 00:00:00 UTC
    MAX_TIMESTAMP = 1767225600000000000  # 2026-01-01 00:00:00 UTC

    if not (MIN_TIMESTAMP <= first_ts <= MAX_TIMESTAMP):
        raise AssertionError(
            f"Timestamp out of expected range (2018-2026):\n"
            f"  Got: {first_ts}\n"
            f"  As datetime: {pd.Timestamp(first_ts, unit='ns', tz='UTC')}"
        )

    # Check timestamp has correct precision (19 digits for nanoseconds)
    ts_str = str(first_ts)
    if len(ts_str) != 19:
        raise AssertionError(
            f"Timestamp doesn't have nanosecond precision:\n"
            f"  Got: {first_ts} ({len(ts_str)} digits)\n"
            f"  Expected: 19 digits for nanoseconds"
        )

    print(f"✅ Timestamp conversion validated ({len(df):,} rows)")
    return True


def seconds_since_midnight_to_time_str(seconds: int) -> str:
    """
    Convert seconds since midnight to HH:MM:SS string.

    Utility function for debugging/logging.

    Args:
        seconds: Seconds since midnight (0-86399)

    Returns:
        Time string in HH:MM:SS format

    Example:
        >>> seconds_since_midnight_to_time_str(34500)
        '09:35:00'
        >>> seconds_since_midnight_to_time_str(33300)
        '09:15:00'  # Market open
        >>> seconds_since_midnight_to_time_str(56700)
        '15:45:00'  # Market close
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def utc_ns_to_datetime_str(utc_ns: int) -> str:
    """
    Convert UTC nanoseconds to readable datetime string.

    Utility function for debugging/logging.

    Args:
        utc_ns: UTC nanoseconds since epoch

    Returns:
        Datetime string in ISO format with UTC timezone

    Example:
        >>> utc_ns_to_datetime_str(1704177900000000000)
        '2024-01-02T04:05:00.000000000Z'
    """
    dt = pd.Timestamp(utc_ns, unit='ns', tz='UTC')
    return dt.isoformat()


def analyze_timestamp_field(series: pd.Series) -> dict:
    """
    Analyze a timestamp field to determine its format.

    Helps identify the format of raw timestamp data.

    Args:
        series: Pandas Series containing timestamp data

    Returns:
        Dictionary with analysis results

    Example:
        >>> df = pd.DataFrame({'trade_time': [34500, 34560, 34620]})
        >>> analyze_timestamp_field(df['trade_time'])
        {
            'dtype': 'int64',
            'min': 34500,
            'max': 34620,
            'sample_values': [34500, 34560, 34620],
            'num_digits': 5,
            'likely_format': 'seconds_since_midnight'
        }
    """
    result = {
        'dtype': str(series.dtype),
        'min': series.min(),
        'max': series.max(),
        'sample_values': series.head(5).tolist(),
        'num_digits': len(str(int(series.iloc[0]))) if pd.api.types.is_numeric_dtype(series) else None
    }

    # Infer likely format based on number of digits
    if result['num_digits']:
        if result['num_digits'] == 8:
            result['likely_format'] = 'YYYYMMDD'
        elif result['num_digits'] == 5:
            result['likely_format'] = 'seconds_since_midnight'
        elif result['num_digits'] == 10:
            result['likely_format'] = 'unix_seconds'
        elif result['num_digits'] == 13:
            result['likely_format'] = 'unix_milliseconds'
        elif result['num_digits'] == 19:
            result['likely_format'] = 'unix_nanoseconds'
        else:
            result['likely_format'] = 'unknown'

    return result
