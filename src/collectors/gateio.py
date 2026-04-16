from datetime import datetime, timezone
from .base import BaseCollector


def _parse_gateio_levels(raw: list) -> list[list[float]]:
    """Gate.io futures depth: levels can be {"p": price_str, "s": size_int} dicts."""
    result = []
    for lvl in raw:
        if isinstance(lvl, dict):
            result.append([float(lvl["p"]), float(lvl["s"])])
        else:
            result.append([float(lvl[0]), float(lvl[1])])
    return result


class GateIOCollector(BaseCollector):
    """
    funding_rate: GET /api/v4/futures/usdt/funding_rate
      Response: list of [{t(unix seconds), r(rate), contract}]
      Note: 'contract' may not be in response — inferred from request param.
    """

    def collect_funding_rates(self) -> list[dict]:
        data = self.fetch("funding_rate")
        symbol = self.get_symbol()
        results = []
        for item in data:
            try:
                results.append({
                    "timestamp": datetime.fromtimestamp(int(item["t"]), tz=timezone.utc),
                    "exchange": "gateio",
                    "symbol": self.instrument["id"],
                    "funding_rate": float(item["r"]),
                    "next_funding_time": None,
                    "mark_price": None,
                    "index_price": None,
                })
            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e} — {item}")
        return results

    def collect_current_funding(self) -> dict:
        # Gate.io: fetch with limit=1 to get latest
        data = self.fetch("funding_rate", extra_params={"limit": 1})
        if not data:
            raise RuntimeError("Gate.io returned no funding rate data")
        item = data[0]
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "gateio",
            "symbol": self.instrument["id"],
            "funding_rate": float(item["r"]),
            "next_funding_time": None,
            "mark_price": None,
            "index_price": None,
        }

    def collect_order_book(self) -> dict:
        # Response: {id:.., current:<unix_ns>, update:.., asks:[{p,s},...], bids:[{p,s},...]}
        data = self.fetch("order_book", section="order_book")
        # timestamp: "current" is unix nanoseconds or seconds; detect by magnitude
        ts_raw = data.get("current") or data.get("update")
        if ts_raw:
            # Gate.io "current" can be nanoseconds (> 1e18) or milliseconds (> 1e12)
            if ts_raw > 1e18:
                ts = datetime.fromtimestamp(ts_raw / 1e9, tz=timezone.utc)
            elif ts_raw > 1e12:
                ts = datetime.fromtimestamp(ts_raw / 1e3, tz=timezone.utc)
            else:
                ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
        bids = _parse_gateio_levels(data["bids"])
        asks = _parse_gateio_levels(data["asks"])
        metrics = self._compute_book_metrics(bids, asks)
        return {
            "timestamp": ts,
            "exchange": "gateio",
            "symbol": self.instrument["id"],
            "bids": bids,
            "asks": asks,
            **metrics,
        }
