"""Research helper for strategy authors.

This module does not place orders and must not gain a broker client. The
trading engine journals intents produced elsewhere. A strategy that needs to
trade implements the Rust `Strategy` trait; this file only documents the
intent shape and checks a few invariants before an idea is ported.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    account_id: str
    strategy_id: str
    instrument_id: str
    side: str
    qty: int
    price: str
    algo_id: str
    product: str = "CNC"

    def validate(self) -> None:
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if self.qty <= 0:
            raise ValueError("qty must be positive")
        if not self.algo_id or len(self.algo_id) > 20:
            raise ValueError("algo id must be 1-20 characters")
        if not self.intent_id or len(self.intent_id) > 20:
            raise ValueError("intent id must be 1-20 characters")
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
        if any(ch not in allowed for ch in self.intent_id):
            raise ValueError("intent id has an illegal character")

    def to_json(self) -> dict:
        self.validate()
        return asdict(self)
