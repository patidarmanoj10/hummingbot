#!/usr/bin/env python

import asyncio

import aiohttp
import logging
import pandas as pd
import uuid
from typing import List, Optional, Dict, Any
import time
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.market.kyber.kyber_active_order_tracker import KyberActiveOrderTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.kyber.kyber_order_book import KyberOrderBook
from hummingbot.core.data_type.order_book_tracker_entry import (
    DDEXOrderBookTrackerEntry,
    OrderBookTrackerEntry
)
from hummingbot.core.data_type.order_book_message import DDEXOrderBookMessage

REST_URL = "https://api.kyber.network"
MARKET_URL = "/market"
BUY_RATE = "/buy_rate"
SELL_RATE = "/sell_rate"


class KyberAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    __daobds__logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.__daobds__logger is None:
            cls.__daobds__logger = logging.getLogger(__name__)
        return cls.__daobds__logger

    def __init__(self, trading_pairs: Optional[List[str]] = None, rest_api_url=""):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self.REST_URL = rest_api_url
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        async with aiohttp.ClientSession() as client:
            markets_response: aiohttp.ClientResponse = await client.get(f"{REST_URL}{MARKET_URL}")
            if markets_response.status != 200:
                raise IOError(f"Error fetching active Kyber markets. HTTP status is {markets_response.status}.")
            markets_data = await markets_response.json()
            markets_data = markets_data["data"]
            field_mapping = {
                "pair": "market",
                "base_symbol": "baseAsset",
                "quote_symbol": "quoteAsset",
                "usd_24h_volume": "USDVolume",
                "base_address": "id"
            }

            all_markets: pd.DataFrame = pd.DataFrame.from_records(
                data=markets_data, index="base_address", columns=list(field_mapping.keys())
            )
            all_markets.rename(field_mapping, axis="columns", inplace=True)
            return all_markets.sort_values("USDVolume", ascending=False)

    @property
    async def exchange_name(self) -> str:
        return "kyber"

    @property
    def order_book_class(self) -> KyberOrderBook:
        return KyberOrderBook

    async def fetch(url, session):
        async with session.get(url) as response:
            return await response.read()

    async def get_snapshot(self, client: aiohttp.ClientSession, base_currency_id: str, level: int = 3) -> Dict[str, any]:
        # TODO: use random number for qty
        params: Dict = {"id": base_currency_id, "qty": 10 }
        bids = []
        asks = []
        for i in range(1):
            async with client.get(f"{REST_URL}{BUY_RATE}", params=params) as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(
                        f"Error fetching Dolomite market snapshot for {base_currency_id}. " f"HTTP status is {response.status}."
                    )
                response = (await response.json())["data"]
                for item in response:
                    bid = {"orderId": str(uuid.uuid4()), "amount": item["dst_qty"][0], "price": item["src_qty"][0] / item["dst_qty"][0]}
                bids.append(bid)

            async with client.get(f"{REST_URL}{SELL_RATE}", params=params) as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(
                        f"Error fetching Dolomite market snapshot for {base_currency_id}. " f"HTTP status is {response.status}."
                    )
                response = (await response.json())["data"]
                for item in response:
                    ask = {"orderId": str(uuid.uuid4()), "amount": item["src_qty"][0], "price": item["dst_qty"][0] / item["src_qty"][0]}
                asks.append(ask)
            response = { "data": { "orderBook": { "marketId": base_currency_id, "bids": bids, "asks": asks }}}
        return response

    async def get_tracking_pairs(self):
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            print('trading_pairs', trading_pairs)
            number_of_pairs: int = len(trading_pairs)
            print('number_of_pairs', number_of_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair, 3)
                    snapshot_timestamp: float = time.time()
                    print('snapshot', snapshot)
                    snapshot_msg = KyberOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        {"marketId": trading_pair}
                    )
                    print('snapshot_msg', snapshot_msg)
                    kyber_order_book: OrderBook = self.order_book_create_function()
                    print('kyber_order_book', kyber_order_book)
                    kyber_active_order_tracker: KyberActiveOrderTracker = KyberActiveOrderTracker()
                    bids, asks = kyber_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    print('bids', bids)
                    print('asks', asks)
                    kyber_order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
                except IOError:
                    self.logger().network(
                        f"Error getting snapshot for {trading_pair}.",
                        exc_info=True,
                        app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
                    )
                    await asyncio.sleep(5.0)
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
                    await asyncio.sleep(5.0)

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass
