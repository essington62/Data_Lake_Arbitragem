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


ORDER_BOOK_SCHEMA = {
    "timestamp": "datetime64[ns, UTC]",
    "exchange": "string",
    "symbol": "string",
    "best_bid": "float64",
    "best_ask": "float64",
    "spread_pct": "float64",
    "mid_price": "float64",
    "bid_depth_usd": "float64",
    "ask_depth_usd": "float64",
    "bids_json": "string",
    "asks_json": "string",
}

ORDER_BOOK_COLUMNS = list(ORDER_BOOK_SCHEMA.keys())
ORDER_BOOK_DEDUP_KEYS = ["timestamp", "exchange", "symbol"]
