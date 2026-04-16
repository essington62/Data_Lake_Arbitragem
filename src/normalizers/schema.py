FUNDING_RATE_SCHEMA = {
    "timestamp": "datetime64[ns, UTC]",
    "exchange": "string",
    "symbol": "string",
    "funding_rate": "float64",
    "next_funding_time": "datetime64[ns, UTC]",  # nullable
    "mark_price": "float64",                      # nullable
    "index_price": "float64",                     # nullable
}

FUNDING_RATE_COLUMNS = list(FUNDING_RATE_SCHEMA.keys())

DEDUP_KEYS = ["timestamp", "exchange", "symbol"]
