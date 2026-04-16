from datetime import datetime, timezone
from .base import BaseCollector


def _ms_to_dt(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class OKXCollector(BaseCollector):
    """
    funding_rate_history: GET /api/v5/public/funding-rate-history
      Response: {code, msg, data: [{instId, fundingRate, fundingTime(ms), ...}]}

    funding_rate_current: GET /api/v5/public/funding-rate
      Response: {code, msg, data: [{instId, fundingRate, fundingTime(ms), nextFundingTime(ms),
                                     markPrice, indexPrice, ...}]}
    """

    def _unwrap(self, response: dict, endpoint_key: str) -> list:
        """OKX wraps all responses in {code, msg, data: [...]}."""
        if response.get("code") != "0":
            raise RuntimeError(f"OKX API error: {response.get('msg', response)}")
        return response.get("data", [])

    def collect_funding_rates(self) -> list[dict]:
        response = self.fetch("funding_rate_history")
        items = self._unwrap(response, "funding_rate_history")
        results = []
        for item in items:
            try:
                results.append({
                    "timestamp": _ms_to_dt(item["fundingTime"]),
                    "exchange": "okx",
                    "symbol": self.instrument["id"],
                    "funding_rate": float(item["fundingRate"]),
                    "next_funding_time": None,
                    "mark_price": None,
                    "index_price": None,
                })
            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e} — {item}")
        return results

    def collect_current_funding(self) -> dict:
        response = self.fetch("funding_rate_current")
        items = self._unwrap(response, "funding_rate_current")
        if not items:
            raise RuntimeError("OKX returned empty data for current funding rate")
        item = items[0]
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "okx",
            "symbol": self.instrument["id"],
            "funding_rate": float(item["fundingRate"]),
            "next_funding_time": _ms_to_dt(item["nextFundingTime"]),
            "mark_price": float(item.get("markPrice") or 0),
            "index_price": float(item.get("indexPrice") or 0),
        }

    def collect_order_book(self) -> dict:
        # Response: {code:"0", data:[{bids:[["price","qty","0","numOrders"],...], asks:[...], ts:"ms"}]}
        response = self.fetch("order_book", section="order_book")
        if response.get("code") != "0":
            raise RuntimeError(f"OKX order book error: {response.get('msg', response)}")
        item = response["data"][0]
        ts = _ms_to_dt(item["ts"])
        # OKX levels: [price, qty, liquidated_orders, num_orders] — take first two
        bids = [[float(lvl[0]), float(lvl[1])] for lvl in item["bids"]]
        asks = [[float(lvl[0]), float(lvl[1])] for lvl in item["asks"]]
        metrics = self._compute_book_metrics(bids, asks)
        return {
            "timestamp": ts,
            "exchange": "okx",
            "symbol": self.instrument["id"],
            "bids": bids,
            "asks": asks,
            **metrics,
        }
