"""Shared provider date parsing helpers."""

from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

from opx_chain.config import US_MARKET_TIMEZONE
from opx_chain.utils import normalize_timestamp


def parse_event_date(raw_date) -> date | None:
    """Convert provider event date values into U.S. market-calendar dates."""
    if raw_date is None:
        return None
    parsed_date = None
    try:
        if pd.isna(raw_date):
            return None
        if isinstance(raw_date, (int, float, np.integer, np.floating)):
            timestamp = normalize_timestamp(raw_date)
            if pd.isna(timestamp):
                return None
            parsed_date = timestamp.tz_convert(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, str):
            parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
        elif isinstance(raw_date, datetime):
            if raw_date.tzinfo is None:
                parsed_date = raw_date.date()
            else:
                parsed_date = raw_date.astimezone(US_MARKET_TIMEZONE).date()
        elif isinstance(raw_date, date):
            parsed_date = raw_date
    except (ValueError, TypeError, OSError):
        pass
    return parsed_date
