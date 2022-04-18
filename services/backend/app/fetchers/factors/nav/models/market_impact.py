from ..utils.contract import ric_to_stem
from ....common.constants import FUTURES

DEFAULT_SPREAD = 5e-4


class MarketImpact:
    def __init__(self):
        pass

    def get(self, stem=None, ric=None):
        if stem is None:
            stem = ric_to_stem(ric)
        return FUTURES.get(stem, {}).get("Spread", DEFAULT_SPREAD)
