from datetime import datetime, timezone
from .base import BaseCollector


def _ms_to_dt(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class BitgetCollector(BaseCollector):
    """
    funding_rate: GET /api/v2/mix/market/history-fund-rate
      Response: {code, msg, data: [{symbol, fundingRate, fundingTime(ms)}]}

    funding_rate_current: GET /api/v2/mix/market/current-fund-rate
      Response: {code, msg, data: {symbol, fundingRate, nextSettlementTime(ms), ...}}
    """

    def _unwrap_list(self, response: dict) -> list:
        if str(response.get("code")) != "00000":
            raise RuntimeError(f"Bitget API error: {response.get('msg', response)}")
        data = response.get("data", [])
        # history endpoint returns list directly
        if isinstance(data, list):
            return data
        return [data]

    def collect_funding_rates(self) -> list[dict]:
        response = self.fetch("funding_rate")
        items = self._unwrap_list(response)
        results = []
        for item in items:
            try:
                results.append({
                    "timestamp": _ms_to_dt(item["fundingTime"]),
                    "exchange": "bitget",
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
        if str(response.get("code")) != "00000":
            raise RuntimeError(f"Bitget API error: {response.get('msg', response)}")
        data = response.get("data", {})
        item = data[0] if isinstance(data, list) else data
        next_ts = item.get("nextSettlementTime") or item.get("nextFundingTime")
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "bitget",
            "symbol": self.instrument["id"],
            "funding_rate": float(item["fundingRate"]),
            "next_funding_time": _ms_to_dt(next_ts) if next_ts else None,
            "mark_price": float(item.get("markPrice") or 0) or None,
            "index_price": None,
        }
