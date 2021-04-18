#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.utils.async_utils import safe_gather

from hummingbot.logger import HummingbotLogger

from hummingbot.connector.exchange.gemini.gemini_order_book import GeminiOrderBook
from hummingbot.connector.exchange.gemini.gemini_active_order_tracker import GeminiActiveOrderTracker
from hummingbot.connector.exchange.gemini.gemini_order_book_tracker_entry import (
    GeminiOrderBookTrackerEntry,
)

GEMINI_REST_URL = "https://api.gemini.com"
GEMINI_WS_FEED = "wss://api.gemini.com"
MAX_RETRIES = 20
NaN = float("nan")


class GeminiAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _gaobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_cache: Dict[str, str] = {}

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._gaobds_logger is None:
            cls._gaobds_logger = logging.getLogger(__name__)
        return cls._gaobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        tasks = [cls.get_last_traded_price(t_pair) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str) -> float:
        # TODO (leina)
        pass
        # async with aiohttp.ClientSession() as client:
        #     ticker_url: str = f"{COINBASE_REST_URL}/products/{trading_pair}/ticker"
        #     resp = await client.get(ticker_url)
        #     resp_json = await resp.json()
        #     return float(resp_json["price"])

    @classmethod
    async def fetch_trading_pairs(cls) -> List[str]:
        # GET https://api.gemini.com/v1/symbols
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{GEMINI_REST_URL}/v1/symbols", timeout=5) as response:
                    if response.status == 200:
                        raw_trading_pairs = await response.json()
                        trading_pair_list: List[str] = []

                        for raw_trading_pair in raw_trading_pairs:
                            # check cache
                            trading_pair = cls._trading_pair_cache.get(raw_trading_pair)
                            if not trading_pair:
                                # if the symbol isn't cached, find the base/quote for each pair
                                details = await cls._fetch_trading_pair_detail(raw_trading_pair)
                                base = details.get("base_currency").upper()
                                quote = details.get("quote_currency").upper()
                                trading_pair = f"{base}-{quote}"
                                cls._trading_pair_cache[raw_trading_pair] = trading_pair

                            trading_pair_list.append(trading_pair)

                        return trading_pair_list

        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for gemini trading pairs
            pass

        return []

    @staticmethod
    async def _fetch_trading_pair_detail(trading_pair: str) -> Dict[str, any]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{GEMINI_REST_URL}/v1/symbols/details/{trading_pair}", timeout=5) as response:
                    if response.status == 200:
                        details = await response.json()
                        return details

        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for gemini trading pairs
            pass

        return {}

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:
        """
        Fetches order book snapshot for a particular trading pair from the rest API
        :returns: Response from the rest API
        """
        # GET https://api.gemini.com/v1/book/:symbol
        symbol = _get_symbol(trading_pair)

        symbol_order_book_url: str = f"{GEMINI_REST_URL}/v1/book/{symbol}"
        async with client.get(symbol_order_book_url) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(
                    f"Error fetching Gemini market snapshot for {trading_pair}. " f"HTTP status is {response.status}."
                )
            data: Dict[str, Any] = await response.json()
            # TODO: clean data (turn strings into ints)??
            data["timestamp"] = time.time()
            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        # TODO (leina)
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = GeminiOrderBook.snapshot_message_from_exchange(
                snapshot, snapshot_timestamp, metadata={"trading_pair": trading_pair}
            )
            active_order_tracker: GeminiActiveOrderTracker = GeminiActiveOrderTracker()
            bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
            return order_book

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        """
        *required
        Initializes order books and order book trackers for the list of trading pairs
        returned by `self.get_trading_pairs`
        :returns: A dictionary of order book trackers for each trading pair
        """
        # TODO (leina)
        pass
        # # Get the currently active markets
        # async with aiohttp.ClientSession() as client:
        #     trading_pairs: List[str] = self._trading_pairs
        #     retval: Dict[str, OrderBookTrackerEntry] = {}

        #     number_of_pairs: int = len(trading_pairs)
        #     for index, trading_pair in enumerate(trading_pairs):
        #         try:
        #             snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
        #             snapshot_timestamp: float = time.time()
        #             snapshot_msg: OrderBookMessage = CoinbaseProOrderBook.snapshot_message_from_exchange(
        #                 snapshot, snapshot_timestamp, metadata={"trading_pair": trading_pair}
        #             )
        #             order_book: OrderBook = self.order_book_create_function()
        #             active_order_tracker: CoinbaseProActiveOrderTracker = CoinbaseProActiveOrderTracker()
        #             bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
        #             order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)

        #             retval[trading_pair] = CoinbaseProOrderBookTrackerEntry(
        #                 trading_pair, snapshot_timestamp, order_book, active_order_tracker
        #             )
        #             self.logger().info(
        #                 f"Initialized order book for {trading_pair}. " f"{index+1}/{number_of_pairs} completed."
        #             )
        #             await asyncio.sleep(0.6)
        #         except IOError:
        #             self.logger().network(
        #                 f"Error getting snapshot for {trading_pair}.",
        #                 exc_info=True,
        #                 app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection.",
        #             )
        #         except Exception:
        #             self.logger().error(f"Error initializing order book for {trading_pair}. ", exc_info=True)
        #     return retval

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        """
        Generator function that returns messages from the web socket stream
        :param ws: current web socket connection
        :returns: message in AsyncIterable format
        """
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Trade messages are received from the order book web socket
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        *required
        Subscribe to diff channel via web socket, and keep the connection open for incoming messages
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """
        # TODO (leina)
        pass
        # while True:
        #     try:
        #         trading_pairs: List[str] = self._trading_pairs
        #         async with websockets.connect(COINBASE_WS_FEED) as ws:
        #             ws: websockets.WebSocketClientProtocol = ws
        #             subscribe_request: Dict[str, Any] = {
        #                 "type": "subscribe",
        #                 "product_ids": trading_pairs,
        #                 "channels": ["full"],
        #             }
        #             await ws.send(ujson.dumps(subscribe_request))
        #             async for raw_msg in self._inner_messages(ws):
        #                 msg = ujson.loads(raw_msg)
        #                 msg_type: str = msg.get("type", None)
        #                 if msg_type is None:
        #                     raise ValueError(f"Coinbase Pro Websocket message does not contain a type - {msg}")
        #                 elif msg_type == "error":
        #                     raise ValueError(f"Coinbase Pro Websocket received error message - {msg['message']}")
        #                 elif msg_type in ["open", "match", "change", "done"]:
        #                     if msg_type == "done" and "price" not in msg:
        #                         # done messages with no price are completed market orders which can be ignored
        #                         continue
        #                     order_book_message: OrderBookMessage = CoinbaseProOrderBook.diff_message_from_exchange(msg)
        #                     output.put_nowait(order_book_message)
        #                 elif msg_type in ["received", "activate", "subscriptions"]:
        #                     # these messages are not needed to track the order book
        #                     continue
        #                 else:
        #                     raise ValueError(f"Unrecognized Coinbase Pro Websocket message received - {msg}")
        #     except asyncio.CancelledError:
        #         raise
        #     except Exception:
        #         self.logger().network(
        #             f'{"Unexpected error with WebSocket connection."}',
        #             exc_info=True,
        #             app_warning_msg=f'{"Unexpected error with WebSocket connection. Retrying in 30 seconds. "}'
        #             f'{"Check network connection."}',
        #         )
        #         await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        *required
        Fetches order book snapshots for each trading pair, and use them to update the local order book
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """
        # TODO (leina)
        pass
        # while True:
        #     try:
        #         trading_pairs: List[str] = self._trading_pairs
        #         async with aiohttp.ClientSession() as client:
        #             for trading_pair in trading_pairs:
        #                 try:
        #                     snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
        #                     snapshot_timestamp: float = time.time()
        #                     snapshot_msg: OrderBookMessage = CoinbaseProOrderBook.snapshot_message_from_exchange(
        #                         snapshot, snapshot_timestamp, metadata={"product_id": trading_pair}
        #                     )
        #                     output.put_nowait(snapshot_msg)
        #                     self.logger().debug(f"Saved order book snapshot for {trading_pair}")
        #                     # Be careful not to go above API rate limits.
        #                     await asyncio.sleep(5.0)
        #                 except asyncio.CancelledError:
        #                     raise
        #                 except Exception:
        #                     self.logger().network(
        #                         f'{"Unexpected error with WebSocket connection."}',
        #                         exc_info=True,
        #                         app_warning_msg=f'{"Unexpected error with WebSocket connection. Retrying in 5 seconds. "}'
        #                         f'{"Check network connection."}',
        #                     )
        #                     await asyncio.sleep(5.0)
        #             this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
        #             next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
        #             delta: float = next_hour.timestamp() - time.time()
        #             await asyncio.sleep(delta)
        #     except asyncio.CancelledError:
        #         raise
        #     except Exception:
        #         self.logger().error("Unexpected error.", exc_info=True)
        #         await asyncio.sleep(5.0)


def _get_symbol(trading_pair: str) -> str:
    return trading_pair.replace("-", "").lower()