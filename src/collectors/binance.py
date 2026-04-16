from datetime import datetime, timezone
from .base import BaseCollector


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _parse_levels(raw: list) -> list[list[float]]:
    """Convert [[price_str, qty_str], ...] → [[float, float], ...]."""
    return [[float(p), float(q)] for p, q in raw]


class BinanceCollector(BaseCollector):
    """
    funding_rate: GET /fapi/v1/fundingRate
      Response: list of {symbol, fundingTime(ms), fundingRate}

    funding_rate_current: GET /fapi/v1/premiumIndex
      Response: dict {symbol, markPrice, indexPrice, lastFundingRate, nextFundingTime(ms)}
    """

    def collect_funding_rates(self) -> list[dict]:
        data = self.fetch("funding_rate")
        mapping = self.config["endpoints"]["funding_rate"]["response_mapping"]
        results = []
        for item in data:
            try:
                results.append({
                    "timestamp": _ms_to_dt(item[mapping["timestamp"]]),
                    "exchange": "binance",
                    "symbol": self.instrument["id"],
                    "funding_rate": float(item[mapping["funding_rate"]]),
                    "next_funding_time": None,
                    "mark_price": None,
                    "index_price": None,
                })
            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e} — {item}")
        return results

    def collect_current_funding(self) -> dict:
        data = self.fetch("funding_rate_current")
        item = data[0] if isinstance(data, list) else data
        mapping = self.config["endpoints"]["funding_rate_current"]["response_mapping"]
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "binance",
            "symbol": self.instrument["id"],
            "funding_rate": float(item[mapping["current_rate"]]),
            "next_funding_time": _ms_to_dt(item[mapping["next_funding_time"]]),
            "mark_price": float(item.get(mapping.get("mark_price", ""), 0) or 0),
            "index_price": float(item.get(mapping.get("index_price", ""), 0) or 0),
        }

    def collect_order_book(self) -> dict:
        # Response: {"lastUpdateId": ..., "T": <ms>, "bids": [["price","qty"],...], "asks": [...]}
        data = self.fetch("order_book", section="order_book")
        ts_raw = data.get("T") or data.get("time")
        ts = _ms_to_dt(ts_raw) if ts_raw else datetime.now(timezone.utc)
        bids = _parse_levels(data["bids"])
        asks = _parse_levels(data["asks"])
        metrics = self._compute_book_metrics(bids, asks)
        return {
            "timestamp": ts,
            "exchange": "binance",
            "symbol": self.instrument["id"],
            "bids": bids,
            "asks": asks,
            **metrics,
        }
