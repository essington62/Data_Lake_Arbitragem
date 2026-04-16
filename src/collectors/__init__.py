from .binance import BinanceCollector
from .okx import OKXCollector
from .bybit import BybitCollector
from .gateio import GateIOCollector
from .bitget import BitgetCollector
from .kucoin import KuCoinCollector

COLLECTOR_MAP = {
    "binance": BinanceCollector,
    "okx": OKXCollector,
    "bybit": BybitCollector,
    "gateio": GateIOCollector,
    "bitget": BitgetCollector,
    "kucoin": KuCoinCollector,
}
