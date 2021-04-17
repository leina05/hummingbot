from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.connector.exchange.gemini.gemini_active_order_tracker import GeminiActiveOrderTracker


class GeminiOrderBookTrackerEntry(OrderBookTrackerEntry):
    def __init__(
        self,
        trading_pair: str,
        timestamp: float,
        order_book: OrderBook,
        active_order_tracker: GeminiActiveOrderTracker,
    ):
        self._active_order_tracker = active_order_tracker
        super(GeminiOrderBookTrackerEntry, self).__init__(trading_pair, timestamp, order_book)

    def __repr__(self) -> str:
        return (
            f"GeminiOrderBookTrackerEntry(trading_pair='{self._trading_pair}', timestamp='{self._timestamp}', "
            f"order_book='{self._order_book}')"
        )

    @property
    def active_order_tracker(self) -> GeminiActiveOrderTracker:
        return self._active_order_tracker
