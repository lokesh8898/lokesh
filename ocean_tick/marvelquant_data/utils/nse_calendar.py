"""
NSE Trading Holiday Calendar for Accurate DTE Calculations.

Provides holiday calendar for NSE (National Stock Exchange of India) to enable:
- Trading day calculations (excludes weekends and holidays)
- Accurate Days-To-Expiry (DTE) calculations
- Holiday-adjusted expiry date calculations

Data Sources:
- https://www.chittorgarh.com/report/india-stock-market-holidays-list-bse-nse/91/
- https://www.angelone.in/nse-holidays-2024
- https://zerodha.com/marketintel/holiday-calendar/

Usage:
    >>> from marvelquant_data.utils.nse_calendar import NSEHolidayCalendar
    >>> calendar = NSEHolidayCalendar()
    >>> calendar.is_trading_day(date(2024, 1, 26))  # Republic Day
    False
    >>> calendar.trading_days_between(date(2024, 1, 15), date(2024, 1, 25))
    7  # Trading days (excludes weekends)
"""

from datetime import date, timedelta
from typing import Set


class NSEHolidayCalendar:
    """
    NSE trading holiday calendar for India market.

    Attributes:
        HOLIDAYS: Set of all NSE holidays from 2018-2024
        WEEKEND_DAYS: Set of weekend day numbers (Saturday=5, Sunday=6)
    """

    # NSE Holidays 2018
    HOLIDAYS_2018 = {
        date(2018, 1, 26),   # Republic Day
        date(2018, 3, 2),    # Maha Shivaratri
        date(2018, 3, 30),   # Good Friday
        date(2018, 4, 2),    # Ram Navami
        date(2018, 5, 1),    # Maharashtra Day
        date(2018, 8, 15),   # Independence Day
        date(2018, 8, 22),   # Bakri Id
        date(2018, 9, 13),   # Ganesh Chaturthi
        date(2018, 10, 2),   # Gandhi Jayanti
        date(2018, 10, 18),  # Dussehra
        date(2018, 11, 7),   # Diwali
        date(2018, 11, 8),   # Diwali Balipratipada
        date(2018, 11, 23),  # Guru Nanak Jayanti
        date(2018, 12, 25),  # Christmas
    }

    # NSE Holidays 2019
    HOLIDAYS_2019 = {
        date(2019, 1, 26),   # Republic Day (Saturday)
        date(2019, 3, 4),    # Maha Shivaratri
        date(2019, 3, 21),   # Holi
        date(2019, 4, 17),   # Ram Navami
        date(2019, 4, 19),   # Good Friday
        date(2019, 5, 1),    # Maharashtra Day
        date(2019, 6, 5),    # Id-ul-Fitr (Ramzan Id)
        date(2019, 8, 12),   # Bakri Id
        date(2019, 8, 15),   # Independence Day
        date(2019, 9, 2),    # Ganesh Chaturthi
        date(2019, 9, 10),   # Moharram
        date(2019, 10, 2),   # Gandhi Jayanti
        date(2019, 10, 8),   # Dussehra
        date(2019, 10, 28),  # Diwali Laxmi Pujan
        date(2019, 11, 12),  # Guru Nanak Jayanti
        date(2019, 12, 25),  # Christmas
    }

    # NSE Holidays 2020
    HOLIDAYS_2020 = {
        date(2020, 1, 26),   # Republic Day (Sunday)
        date(2020, 2, 21),   # Maha Shivaratri
        date(2020, 3, 10),   # Holi
        date(2020, 4, 2),    # Ram Navami
        date(2020, 4, 6),    # Mahavir Jayanti
        date(2020, 4, 10),   # Good Friday
        date(2020, 5, 1),    # Maharashtra Day
        date(2020, 5, 25),   # Id-ul-Fitr (Ramzan Id)
        date(2020, 8, 1),    # Bakri Id
        date(2020, 8, 15),   # Independence Day (Saturday)
        date(2020, 10, 2),   # Gandhi Jayanti
        date(2020, 10, 25),  # Dussehra (Sunday)
        date(2020, 11, 14),  # Diwali Balipratipada (Saturday)
        date(2020, 11, 16),  # Diwali Laxmi Pujan
        date(2020, 11, 30),  # Guru Nanak Jayanti
        date(2020, 12, 25),  # Christmas
    }

    # NSE Holidays 2021
    HOLIDAYS_2021 = {
        date(2021, 1, 26),   # Republic Day
        date(2021, 3, 11),   # Maha Shivaratri
        date(2021, 3, 29),   # Holi
        date(2021, 4, 2),    # Good Friday
        date(2021, 4, 21),   # Ram Navami
        date(2021, 5, 13),   # Id-ul-Fitr (Ramzan Id)
        date(2021, 7, 21),   # Bakri Id
        date(2021, 8, 19),   # Moharram
        date(2021, 9, 10),   # Ganesh Chaturthi
        date(2021, 10, 15),  # Dussehra
        date(2021, 11, 4),   # Diwali Laxmi Pujan
        date(2021, 11, 5),   # Diwali Balipratipada
        date(2021, 11, 19),  # Guru Nanak Jayanti
    }

    # NSE Holidays 2022
    HOLIDAYS_2022 = {
        date(2022, 1, 26),   # Republic Day
        date(2022, 3, 1),    # Maha Shivaratri
        date(2022, 3, 18),   # Holi
        date(2022, 4, 14),   # Dr. Baba Saheb Ambedkar Jayanti
        date(2022, 4, 15),   # Good Friday
        date(2022, 5, 3),    # Id-ul-Fitr (Ramzan Id)
        date(2022, 7, 10),   # Bakri Id
        date(2022, 8, 9),    # Moharram
        date(2022, 8, 15),   # Independence Day
        date(2022, 8, 31),   # Ganesh Chaturthi
        date(2022, 10, 5),   # Dussehra
        date(2022, 10, 24),  # Diwali Laxmi Pujan
        date(2022, 10, 26),  # Diwali Balipratipada
        date(2022, 11, 8),   # Guru Nanak Jayanti
    }

    # NSE Holidays 2023
    HOLIDAYS_2023 = {
        date(2023, 1, 26),   # Republic Day
        date(2023, 3, 7),    # Holi
        date(2023, 3, 30),   # Ram Navami
        date(2023, 4, 4),    # Mahavir Jayanti
        date(2023, 4, 7),    # Good Friday
        date(2023, 4, 14),   # Dr. Baba Saheb Ambedkar Jayanti
        date(2023, 4, 22),   # Id-ul-Fitr (Ramzan Id)
        date(2023, 5, 1),    # Maharashtra Day
        date(2023, 6, 29),   # Bakri Id
        date(2023, 8, 15),   # Independence Day
        date(2023, 9, 19),   # Ganesh Chaturthi
        date(2023, 10, 2),   # Gandhi Jayanti
        date(2023, 10, 24),  # Dussehra
        date(2023, 11, 12),  # Diwali Balipratipada
        date(2023, 11, 13),  # Diwali Laxmi Pujan
        date(2023, 11, 27),  # Guru Nanak Jayanti
        date(2023, 12, 25),  # Christmas
    }

    # NSE Holidays 2024
    HOLIDAYS_2024 = {
        date(2024, 1, 26),   # Republic Day
        date(2024, 3, 8),    # Maha Shivaratri
        date(2024, 3, 25),   # Holi
        date(2024, 3, 29),   # Good Friday
        date(2024, 4, 11),   # Id-ul-Fitr (Ramzan Id)
        date(2024, 4, 17),   # Ram Navami
        date(2024, 5, 1),    # Maharashtra Day
        date(2024, 6, 17),   # Bakri Id
        date(2024, 7, 17),   # Moharram
        date(2024, 8, 15),   # Independence Day
        date(2024, 10, 2),   # Gandhi Jayanti
        date(2024, 11, 1),   # Diwali Laxmi Pujan
        date(2024, 11, 15),  # Guru Nanak Jayanti
        date(2024, 12, 25),  # Christmas
    }

    # Combined set of all holidays
    HOLIDAYS = (
        HOLIDAYS_2018 | HOLIDAYS_2019 | HOLIDAYS_2020 |
        HOLIDAYS_2021 | HOLIDAYS_2022 | HOLIDAYS_2023 | HOLIDAYS_2024
    )

    # Weekend days (Saturday=5, Sunday=6)
    WEEKEND_DAYS = {5, 6}

    def is_trading_day(self, dt: date) -> bool:
        """
        Check if a given date is a trading day.

        A trading day is:
        - Not a weekend (Saturday/Sunday)
        - Not a declared NSE holiday

        Args:
            dt: Date to check

        Returns:
            True if trading day, False otherwise

        Example:
            >>> calendar = NSEHolidayCalendar()
            >>> calendar.is_trading_day(date(2024, 1, 26))  # Republic Day
            False
            >>> calendar.is_trading_day(date(2024, 1, 15))  # Monday
            True
        """
        return (
            dt.weekday() not in self.WEEKEND_DAYS
            and dt not in self.HOLIDAYS
        )

    def trading_days_between(self, start: date, end: date) -> int:
        """
        Count trading days between start (inclusive) and end (exclusive).

        Used for accurate DTE (Days-To-Expiry) calculations that exclude
        weekends and holidays (options don't decay on non-trading days).

        Args:
            start: Start date (inclusive)
            end: End date (exclusive)

        Returns:
            Number of trading days

        Example:
            >>> calendar = NSEHolidayCalendar()
            >>> # Jan 15 (Mon) to Jan 25 (Thu) 2024
            >>> calendar.trading_days_between(date(2024, 1, 15), date(2024, 1, 25))
            7  # Excludes 2 weekends (Jan 20-21)
        """
        count = 0
        current = start
        while current < end:
            if self.is_trading_day(current):
                count += 1
            current += timedelta(days=1)
        return count

    def get_previous_trading_day(self, dt: date) -> date:
        """
        Get the previous trading day before a given date.

        Used for holiday adjustment of expiry dates (if expiry falls on
        holiday, move to previous trading day).

        Args:
            dt: Reference date

        Returns:
            Previous trading day

        Example:
            >>> calendar = NSEHolidayCalendar()
            >>> # Republic Day 2024 (Friday, Jan 26)
            >>> calendar.get_previous_trading_day(date(2024, 1, 26))
            date(2024, 1, 25)  # Thursday
        """
        current = dt - timedelta(days=1)
        while not self.is_trading_day(current):
            current -= timedelta(days=1)
        return current

    def get_next_trading_day(self, dt: date) -> date:
        """
        Get the next trading day after a given date.

        Args:
            dt: Reference date

        Returns:
            Next trading day

        Example:
            >>> calendar = NSEHolidayCalendar()
            >>> # Friday Jan 26, 2024 (Republic Day)
            >>> calendar.get_next_trading_day(date(2024, 1, 26))
            date(2024, 1, 29)  # Monday (skips weekend)
        """
        current = dt + timedelta(days=1)
        while not self.is_trading_day(current):
            current += timedelta(days=1)
        return current

    def get_trading_days_in_month(self, year: int, month: int) -> list[date]:
        """
        Get all trading days in a given month.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)

        Returns:
            List of trading days in the month

        Example:
            >>> calendar = NSEHolidayCalendar()
            >>> trading_days = calendar.get_trading_days_in_month(2024, 1)
            >>> len(trading_days)
            22  # Typical month has ~22 trading days
        """
        import calendar as cal

        trading_days = []
        last_day = cal.monthrange(year, month)[1]

        for day in range(1, last_day + 1):
            dt = date(year, month, day)
            if self.is_trading_day(dt):
                trading_days.append(dt)

        return trading_days
