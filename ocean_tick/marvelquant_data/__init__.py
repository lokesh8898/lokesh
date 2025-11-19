"""
MarvelQuant Data Management Library

A standalone Python library for managing and transforming NSE (National Stock Exchange of India)
market data for use with NautilusTrader backtesting framework.

Features:
- Custom data types (OptionOI, FutureOI) for open interest tracking
- NSE-specific utilities (lot sizes, holiday calendar, expiry calculations)
- Data transformation scripts for converting raw NSE data to Nautilus format
- Sample data for testing and development

Usage:
    >>> from marvelquant_data.data_types import OptionOI, FutureOI
    >>> from marvelquant_data.utils import NSEHolidayCalendar, create_options_contract
    >>> from marvelquant_data.transformers import NautilusDataTransformer

For more information, see the documentation in the docs/ directory.
"""

__version__ = "0.1.0"
__author__ = "MarvelQuant"
__license__ = "MIT"

# Import main modules for convenient access
from . import data_types
from . import utils

__all__ = [
    "data_types",
    "utils",
]
