"""Paper quote pricing for Silver Micro."""

import math


def quote_fill(quote, side, quantity):
    if side not in ("BUY", "SELL"):
        raise ValueError("Paper order side must be BUY or SELL")
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        raise ValueError("Paper order quantity must be a positive whole number")
    book = {}
    for kind in ("buy", "sell"):
        levels = []
        for row in quote["depth"][kind]:
            price = float(row["price"])
            size = float(row["quantity"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid market depth price")
            if not math.isfinite(size) or size < 0 or int(size) != size:
                raise ValueError("Invalid market depth quantity")
            if size > 0:
                levels.append((price, int(size)))
        if not levels:
            raise ValueError("Missing executable bid or ask")
        levels.sort(reverse=kind == "buy")
        book[kind] = levels[0]
    bid, bid_size = book["buy"]
    ask, ask_size = book["sell"]
    if bid > ask:
        raise ValueError("Crossed market depth")
    price = ask if side == "BUY" else bid
    available = ask_size if side == "BUY" else bid_size
    return dict(price=price, bid=bid, ask=ask, bid_size=bid_size,
                ask_size=ask_size, spread=round(ask - bid, 2),
                insufficient_best_size=available < quantity)
