#!/usr/bin/env python

from aiokafka import ConsumerRecord
from decimal import Decimal
import logging
from typing import (
    Dict,
    List,
    Optional,
)
import ujson

from hummingbot.core.event.events import TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    KyberOrderBookMessage,
    OrderBookMessage,
    OrderBookMessageType
)

_dob_logger = None


cdef class KyberOrderBook(OrderBook):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _dob_logger
        if _dob_logger is None:
            _dob_logger = logging.getLogger(__name__)
        return _dob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> KyberOrderBookMessage:
        if metadata:
            msg["data"]["orderBook"].update(metadata)
        return KyberOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg["data"]["orderBook"], timestamp)
#
#    @classmethod
#    def diff_message_from_exchange(cls,
#                                   msg: Dict[str, any],
#                                   timestamp: Optional[float] = None,
#                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
#        if metadata:
#            msg.update(metadata)
#        return KyberOrderBookMessage(OrderBookMessageType.DIFF, msg, timestamp)
#
#    @classmethod
#    def trade_message_from_exchange(cls,
#                                   msg: Dict[str, any],
#                                   timestamp: Optional[float] = None,
#                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
#        if metadata:
#            msg.update(metadata)
#        return KyberOrderBookMessage(OrderBookMessageType.TRADE, {
#            "marketId": msg["marketId"],
#            "trade_type": float(TradeType.SELL.value) if msg["makerSide"] == "sell" else float(TradeType.BUY.value),
#            "price": msg["price"],
#            "amount": msg["amount"],
#            "time": msg["time"]
#        }, timestamp)
#
#    @classmethod
#    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
#        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
#        return KyberOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg["data"]["orderBook"],
#                                    timestamp=record.timestamp * 1e-3)
#
#    @classmethod
#    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
#        return KyberOrderBookMessage(OrderBookMessageType.DIFF, record.json)
#
#    @classmethod
#    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
#        msg = ujson.loads(record.value.decode())
#        return KyberOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg,
#                                    timestamp=record.timestamp * 1e-3)
#
#    @classmethod
#    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
#        msg = ujson.loads(record.value.decode())
#        return KyberOrderBookMessage(OrderBookMessageType.DIFF, msg)
#
#    @classmethod
#    def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
#        return KyberOrderBookMessage(OrderBookMessageType.TRADE, record.json)
#
#    @classmethod
#    def from_snapshot(cls, snapshot: OrderBookMessage):
#        raise NotImplementedError("Kyber order book needs to retain individual order data.")
#
#    @classmethod
#    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
#        raise NotImplementedError("Kyber order book needs to retain individual order data.")
