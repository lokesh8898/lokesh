"""
Contract Metadata Generators for Nautilus Instruments.

Generates proper OptionsContract and FuturesContract metadata following
Nautilus standards for NSE instruments.

Handles:
- Symbol parsing (BANKNIFTY28OCT2548000CE → components)
- InstrumentId generation ({SYMBOL}.NSE format)
- Lot size mapping per underlying
- Contract metadata population

Usage:
    >>> from marvelquant_data.utils.contract_generators import create_options_contract
    >>> from datetime import date
    >>>
    >>> contract = create_options_contract(
    ...     symbol_str="BANKNIFTY28OCT2548000CE",
    ...     strike=48000.0,
    ...     expiry=date(2024, 10, 28),
    ...     option_type="CALL",
    ...     underlying="BANKNIFTY"
    ... )
"""

from datetime import date, datetime
from decimal import Decimal
import pandas as pd
import pytz

from nautilus_trader.model.instruments import OptionContract, FuturesContract
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.model.enums import AssetClass, OptionKind


# NSE lot sizes per underlying (as of 2024)
NSE_LOT_SIZES = {
    # Index Futures/Options
    "NIFTY": 25,
    "BANKNIFTY": 15,
    "FINNIFTY": 25,
    "MIDCPNIFTY": 50,
    "SENSEX": 10,
    "BANKEX": 15,

    # Commodity Futures/Options
    "CRUDEOIL": 100,
    "NATURALGAS": 1250,
    "GOLD": 100,
    "SILVER": 30,
    "COPPER": 1000,
    "ZINC": 1000,
    "LEAD": 1000,
    "ALUMINIUM": 1000,
    "NICKEL": 250,
}

# Asset class mapping
ASSET_CLASS_MAP = {
    # Commodities
    "CRUDEOIL": AssetClass.COMMODITY,
    "NATURALGAS": AssetClass.COMMODITY,
    "GOLD": AssetClass.COMMODITY,
    "SILVER": AssetClass.COMMODITY,
    "COPPER": AssetClass.COMMODITY,
    "ZINC": AssetClass.COMMODITY,
    "LEAD": AssetClass.COMMODITY,
    "ALUMINIUM": AssetClass.COMMODITY,
    "NICKEL": AssetClass.COMMODITY,

    # Everything else defaults to EQUITY (indices, stocks)
    # NIFTY, BANKNIFTY, FINNIFTY, etc.
}


def create_options_contract(
    symbol: str,
    strike: float,
    expiry: date,
    option_kind: str,
    underlying: str,
    lot_size: int = None,
    venue: str = "NSE"
) -> OptionContract:
    """
    Create Nautilus OptionContract following official pattern.

    Based on: nautilus_trader/test_kit/providers.py::aapl_option()

    Args:
        symbol: NSE option symbol (without .NSE suffix)
        strike: Strike price
        expiry: Expiry date
        option_kind: "CALL" or "PUT"
        underlying: Underlying symbol (NIFTY, BANKNIFTY, etc.)
        lot_size: Lot size (optional, auto-detected)
        venue: Exchange venue (default: NSE)

    Returns:
        OptionContract instance
    """
    # Determine lot size
    if lot_size is None:
        lot_size = NSE_LOT_SIZES.get(underlying.upper(), 1)

    # Determine asset class (Commodity vs Equity)
    asset_class = ASSET_CLASS_MAP.get(underlying.upper(), AssetClass.EQUITY)

    # Create InstrumentId
    instrument_id = InstrumentId(
        symbol=Symbol(symbol),
        venue=Venue(venue)
    )

    # Parse option kind
    kind = OptionKind.CALL if option_kind.upper() in ["CALL", "CE"] else OptionKind.PUT

    # Convert expiry to UTC timestamp (nanoseconds)
    expiry_utc = pd.Timestamp(expiry, tz=pytz.utc)

    # Set activation to 30 days before expiry (options should be tradeable before expiry!)
    activation_utc = expiry_utc - pd.Timedelta(days=30)

    # Create OptionContract (following Nautilus test provider pattern)
    # Use SPOT INDEX as underlying for NSE index options Greeks calculation
    # NSE index options (NIFTY, BANKNIFTY) reference spot index, NOT futures
    # Futures price includes carry cost (interest - dividend), options reference spot
    underlying_spot = f"{underlying}-INDEX" if underlying.upper() in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"] else underlying

    contract = OptionContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        asset_class=asset_class,  # Proper classification
        exchange="NSE",  # Exchange as string
        underlying=underlying_spot,  # Use spot index for Greeks calculation
        option_kind=kind,
        activation_ns=activation_utc.value,  # 30 days before expiry
        expiration_ns=expiry_utc.value,
        strike_price=Price.from_str(f"{strike:.2f}"),
        currency=Currency.from_str("INR"),
        price_precision=2,
        price_increment=Price.from_str("0.05"),
        multiplier=Quantity.from_int(lot_size),  # NSE: multiplier = lot_size (e.g., 50 for NIFTY)
        lot_size=Quantity.from_int(lot_size),
        ts_event=0,
        ts_init=0,
    )

    return contract


def create_futures_contract(
    symbol: str,
    expiry_date: str | date,
    underlying: str,
    lot_size: int = None,
    venue: str = "NSE"
) -> FuturesContract:
    """
    Create Nautilus FuturesContract following official pattern.

    Based on: nautilus_trader/test_kit/providers.py::es_future()

    Args:
        symbol: Futures symbol (e.g., "NIFTY-I" or "NIFTY28MAR24")
        expiry_date: Expiry date or "continuous" for continuous contract
        underlying: Underlying symbol (NIFTY, BANKNIFTY, etc.)
        lot_size: Lot size (optional, auto-detected)
        venue: Exchange venue (default: NSE)

    Returns:
        FuturesContract instance
    """
    # Determine lot size
    if lot_size is None:
        lot_size = NSE_LOT_SIZES.get(underlying.upper(), 1)

    # Determine asset class (Commodity vs Equity)
    asset_class = ASSET_CLASS_MAP.get(underlying.upper(), AssetClass.EQUITY)

    # Create InstrumentId
    instrument_id = InstrumentId(
        symbol=Symbol(symbol),
        venue=Venue(venue)
    )

    # Handle expiry timestamp
    if isinstance(expiry_date, str) and expiry_date == "continuous":
        # For continuous contracts, use far future date
        expiry_utc = pd.Timestamp("2099-12-31", tz=pytz.utc)
        activation_utc = pd.Timestamp("2020-01-01", tz=pytz.utc)  # Far past for continuous
    elif isinstance(expiry_date, date):
        expiry_utc = pd.Timestamp(expiry_date, tz=pytz.utc)
        activation_utc = expiry_utc - pd.Timedelta(days=90)  # 90 days before expiry for futures
    else:
        expiry_utc = pd.Timestamp(expiry_date, tz=pytz.utc)
        activation_utc = expiry_utc - pd.Timedelta(days=90)  # 90 days before expiry for futures

    # Create FuturesContract (following Nautilus test provider pattern)
    contract = FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        asset_class=asset_class,  # Proper classification
        exchange="NSE",  # Exchange as string
        underlying=underlying,
        activation_ns=activation_utc.value,  # Set before expiry for trading
        expiration_ns=expiry_utc.value,
        currency=Currency.from_str("INR"),
        price_precision=2,
        price_increment=Price.from_str("0.05"),
        multiplier=Quantity.from_int(1),
        lot_size=Quantity.from_int(lot_size),
        ts_event=0,
        ts_init=0,
    )

    return contract


def _coerce_expiry(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(value, dayfirst=True).date()
    except Exception as exc:
        raise ValueError(f"Unable to coerce expiry value {value!r} to date") from exc


def parse_nse_option_symbol(
    symbol: str,
    expiry_hint: date | datetime | str | None = None,
    strike_hint: float | int | str | None = None,
    option_type_hint: str | None = None,
) -> dict:
    """
    Parse NSE option symbol into components.

    Format: {UNDERLYING}{DDMMMYY}{STRIKE}{CE|PE}
    Example: BANKNIFTY28OCT2548000CE

    Args:
        symbol: NSE option symbol
        expiry_hint: Optional expiry override if parsing fails or is ambiguous
        strike_hint: Optional strike override if parsing fails
        option_type_hint: Optional option type override ("CALL"/"PUT") for non-standard codes

    Returns:
        Dictionary with components:
        - underlying: Underlying symbol
        - expiry: Expiry date
        - strike: Strike price
        - option_type: "CALL" or "PUT"

    Example:
        >>> parse_nse_option_symbol("BANKNIFTY28OCT2548000CE")
        {
            'underlying': 'BANKNIFTY',
            'expiry': date(2024, 10, 28),
            'strike': 48000.0,
            'option_type': 'CALL'
        }
    """
    # Extract option type (last 2 chars: CE or PE)
    option_type_code = symbol[-2:].upper()
    if option_type_code in ("CE", "PE"):
        option_type = "CALL" if option_type_code == "CE" else "PUT"
    elif option_type_hint:
        option_type = option_type_hint.upper()
    else:
        raise ValueError(f"Unsupported option type code in {symbol}")

    # Remove option type from symbol
    symbol_without_type = symbol[:-2]

    # Extract strike (last 5 digits before CE/PE)
    strike_part = symbol_without_type[-5:]
    try:
        strike = float(strike_part)
    except ValueError:
        if strike_hint is None:
            raise
        strike = float(strike_hint)

    # Extract date (7 chars before strike: DDMMMYY)
    date_str = symbol_without_type[-12:-5]  # e.g., "28OCT25"

    # Parse expiry date (fall back to hint when parsing fails)
    try:
        expiry = datetime.strptime(date_str, "%d%b%y").date()
    except ValueError:
        expiry = _coerce_expiry(expiry_hint)
        if expiry is None:
            raise

    # Extract underlying (everything before date)
    underlying = symbol_without_type[:-12]

    return {
        'underlying': underlying,
        'expiry': expiry,
        'strike': strike,
        'option_type': option_type
    }
