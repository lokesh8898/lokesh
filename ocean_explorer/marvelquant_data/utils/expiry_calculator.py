"""
NSE Expiry Calculator and Expiry Bucket Classification.

Provides functions for:
- Calculating NSE option/future expiry dates (last Thursday rule)
- Holiday-adjusted expiry dates
- Expiry bucket classification (CW/NW/CM/NM)
- Weekly expiry calculations

NSE Expiry Rules:
- Monthly Options/Futures: Last Thursday of month
- Weekly Options: Every Thursday
- Holiday Adjustment: If Thursday is holiday, move to previous trading day

Usage:
    >>> from marvelquant_data.utils.expiry_calculator import get_nse_monthly_expiry
    >>> from marvelquant_data.utils.nse_calendar import NSEHolidayCalendar
    >>> calendar = NSEHolidayCalendar()
    >>> get_nse_monthly_expiry(2024, 1, calendar)
    date(2024, 1, 25)  # Last Thursday of January 2024
"""

from datetime import date, timedelta
import calendar as cal
from typing import List

from .nse_calendar import NSEHolidayCalendar


def get_nse_monthly_expiry(year: int, month: int, calendar: NSEHolidayCalendar) -> date:
    """
    Calculate NSE monthly expiry (last Thursday of month, adjusted for holidays).

    NSE Rule: Last Thursday of month. If Thursday is holiday, move to
    previous trading day.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        calendar: NSEHolidayCalendar instance

    Returns:
        Expiry date (last trading Thursday)

    Example:
        >>> calendar = NSEHolidayCalendar()
        >>> get_nse_monthly_expiry(2024, 1, calendar)
        date(2024, 1, 25)  # Last Thursday of Jan 2024
        >>> get_nse_monthly_expiry(2024, 10, calendar)
        date(2024, 10, 31)  # Last Thursday of Oct 2024
    """
    # Get last day of month
    last_day_num = cal.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)

    # Find last Thursday (weekday 3 = Thursday, 0=Monday, 6=Sunday)
    # days_since_thursday tells us how many days back from last_day to Thursday
    days_since_thursday = (last_day.weekday() - 3) % 7
    last_thursday = last_day - timedelta(days=days_since_thursday)

    # Adjust for holidays (move backwards to previous trading day)
    while not calendar.is_trading_day(last_thursday):
        last_thursday -= timedelta(days=1)

    return last_thursday


def get_nse_weekly_expiries(year: int, month: int, calendar: NSEHolidayCalendar) -> List[date]:
    """
    Get all NSE weekly expiries (Thursdays) in a given month.

    NSE Rule: Weekly options expire every Thursday.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        calendar: NSEHolidayCalendar instance

    Returns:
        List of weekly expiry dates (all Thursdays in month, holiday-adjusted)

    Example:
        >>> calendar = NSEHolidayCalendar()
        >>> weekly_expiries = get_nse_weekly_expiries(2024, 1, calendar)
        >>> len(weekly_expiries)
        4  # January 2024 has 4 Thursdays
    """
    expiries = []
    last_day_num = cal.monthrange(year, month)[1]
    first_day = date(year, month, 1)
    last_day = date(year, month, last_day_num)

    current = first_day
    while current <= last_day:
        # Thursday = weekday 3
        if current.weekday() == 3:
            # Adjust for holidays
            expiry = current
            while not calendar.is_trading_day(expiry):
                expiry -= timedelta(days=1)
            expiries.append(expiry)
        current += timedelta(days=1)

    return expiries


def classify_expiry_bucket(
    option_expiry: date,
    current_date: date,
    calendar: NSEHolidayCalendar
) -> str:
    """
    Classify option into expiry bucket based on trading DTE.

    Buckets (based on trading days, not calendar days):
    - CW: Current Week (≤7 trading days) - 0DTE strategies, weekly spreads
    - NW: Next Week (8-14 trading days) - short-term credit spreads
    - CM: Current Month (15-30 trading days) - monthly iron condors
    - NM: Next Month (31+ trading days) - LEAPS, diagonal spreads

    Args:
        option_expiry: Option expiry date
        current_date: Current date for DTE calculation
        calendar: NSEHolidayCalendar instance

    Returns:
        Expiry bucket code: "CW" | "NW" | "CM" | "NM"

    Example:
        >>> calendar = NSEHolidayCalendar()
        >>> # 7 trading days to expiry
        >>> classify_expiry_bucket(
        ...     option_expiry=date(2024, 1, 25),
        ...     current_date=date(2024, 1, 15),
        ...     calendar=calendar
        ... )
        'CW'
        >>> # 25 trading days to expiry
        >>> classify_expiry_bucket(
        ...     option_expiry=date(2024, 2, 29),
        ...     current_date=date(2024, 1, 25),
        ...     calendar=calendar
        ... )
        'CM'
    """
    # Calculate DTE using trading days (excludes weekends and holidays)
    dte = calendar.trading_days_between(current_date, option_expiry)

    if dte <= 7:
        return "CW"
    elif dte <= 14:
        return "NW"
    elif dte <= 30:
        return "CM"
    else:
        return "NM"


def get_expiry_bucket_description(bucket: str) -> str:
    """
    Get human-readable description of expiry bucket.

    Args:
        bucket: Bucket code ("CW", "NW", "CM", "NM")

    Returns:
        Description string

    Example:
        >>> get_expiry_bucket_description("CW")
        'Current Week (≤7 DTE) - 0DTE strategies, weekly spreads'
    """
    descriptions = {
        "CW": "Current Week (≤7 DTE) - 0DTE strategies, weekly spreads",
        "NW": "Next Week (8-14 DTE) - short-term credit spreads",
        "CM": "Current Month (15-30 DTE) - monthly iron condors",
        "NM": "Next Month (31+ DTE) - LEAPS, diagonal spreads"
    }
    return descriptions.get(bucket, "Unknown bucket")


def get_all_monthly_expiries(year: int, calendar: NSEHolidayCalendar) -> dict[int, date]:
    """
    Get all monthly expiries for a given year.

    Args:
        year: Year (e.g., 2024)
        calendar: NSEHolidayCalendar instance

    Returns:
        Dictionary mapping month (1-12) to expiry date

    Example:
        >>> calendar = NSEHolidayCalendar()
        >>> expiries = get_all_monthly_expiries(2024, calendar)
        >>> expiries[1]
        date(2024, 1, 25)  # January expiry
        >>> expiries[12]
        date(2024, 12, 26)  # December expiry
    """
    expiries = {}
    for month in range(1, 13):
        expiries[month] = get_nse_monthly_expiry(year, month, calendar)
    return expiries


def is_expiry_day(dt: date, calendar: NSEHolidayCalendar) -> bool:
    """
    Check if a given date is a monthly expiry day.

    Args:
        dt: Date to check
        calendar: NSEHolidayCalendar instance

    Returns:
        True if date is monthly expiry, False otherwise

    Example:
        >>> calendar = NSEHolidayCalendar()
        >>> is_expiry_day(date(2024, 1, 25), calendar)  # Last Thursday Jan
        True
        >>> is_expiry_day(date(2024, 1, 24), calendar)  # Wednesday
        False
    """
    monthly_expiry = get_nse_monthly_expiry(dt.year, dt.month, calendar)
    return dt == monthly_expiry
