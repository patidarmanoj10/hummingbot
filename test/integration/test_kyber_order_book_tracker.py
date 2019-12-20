#!/usr/bin/env python
import math
import time
from os.path import (
    join,
    realpath
)
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    OrderBookTradeEvent,
    TradeType,
    OrderBookEvent
)
import asyncio
import logging
import unittest
from typing import (
    Dict,
    Optional,
    List
)
from hummingbot.market.kyber.kyber_order_book_tracker import KyberOrderBookTracker
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTrackerDataSourceType
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)


class KyberOrderBookTrackerUnitTest(unittest.TestCase):
    order_book_tracker: Optional[KyberOrderBookTracker] = None
    events: List[OrderBookEvent] = [
        OrderBookEvent.TradeEvent
    ]
    #todo: change trading pair to ETH-MET format
    trading_pairs: List[str] = [ "0xdd974d5c2e2928dea5f71b9825b8b646686bd200" ]

    @classmethod
    def setUpClass(cls):
        print('2')
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_tracker: KyberOrderBookTracker = KyberOrderBookTracker(
            data_source_type=OrderBookTrackerDataSourceType.EXCHANGE_API,
            trading_pairs=cls.trading_pairs)
        cls.order_book_tracker_task: asyncio.Task = safe_ensure_future(cls.order_book_tracker.start())
        cls.ev_loop.run_until_complete(cls.wait_til_tracker_ready())

    @classmethod
    async def wait_til_tracker_ready(cls):
        while True:
            print('3')
            if len(cls.order_book_tracker.order_books) > 0:
                print("Initialized real-time order books.")
                return
            await asyncio.sleep(1)

    async def run_parallel_async(self, *tasks, timeout=None):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        timer = 0
        print('4')
        while not future.done():
            if timeout and timer > timeout:
                raise Exception("Time out running parallel async task in tests.")
            timer += 1
            now = time.time()
            next_iteration = now // 1.0 + 1
            await asyncio.sleep(1.0)
        return future.result()

    def run_parallel(self, *tasks, timeout=None):
        print('5')
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks, timeout=timeout))

    def setUp(self):
        print('6')
        self.event_logger = EventLogger()
        for event_tag in self.events:
            for trading_pair, order_book in self.order_book_tracker.order_books.items():
                order_book.add_listener(event_tag, self.event_logger)

    @unittest.skipUnless(any("test_order_book_trade_event_emission" in arg for arg in sys.argv),
                         "test_order_book_trade_event_emission test requires waiting or manual trade.")

    def test_tracker_integrity(self):
        # Wait 5 seconds to process some diffs.
        print('11')
        self.ev_loop.run_until_complete(asyncio.sleep(5.0))
        order_books: Dict[str, OrderBook] = self.order_book_tracker.order_books
        usdc_dai_book: OrderBook = order_books["0xdd974d5c2e2928dea5f71b9825b8b646686bd200"]
        # weth_dai_book: OrderBook = order_books["WETH-DAI"]
        # print("usdc_dai_book")
        # print(usdc_dai_book.snapshot)
        # print("weth_dai_book")
        # print(weth_dai_book.snapshot)
        self.assertGreaterEqual(usdc_dai_book.get_price_for_volume(True, 10).result_price,
                                usdc_dai_book.get_price(True))
        self.assertLessEqual(usdc_dai_book.get_price_for_volume(False, 10).result_price,
                             usdc_dai_book.get_price(False))
        # self.assertGreaterEqual(weth_dai_book.get_price_for_volume(True, 10).result_price,
        #                         weth_dai_book.get_price(True))
        # self.assertLessEqual(weth_dai_book.get_price_for_volume(False, 10).result_price,
        #                      weth_dai_book.get_price(False))


def main():
    print('8')
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
