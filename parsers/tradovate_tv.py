from config     import TS_FMT, FUT_DEFS
from enum       import IntEnum
from polars     import col, Datetime, read_csv


class trade_row(IntEnum):

    b_s         = 3
    product     = 5
    avg_price   = 7
    filled_qty  = 8
    fill_time   = 9
    status      = 11


TV_DT_FMT = "%m/%d/%Y %H:%M:%S"


def parse(in_fn: str):

    trades      = read_csv(in_fn)
    trades      = trades.with_columns(
                    [
                        col("Fill Time").str.strptime(Datetime, TV_DT_FMT).dt.strftime(TS_FMT).alias("Fill Time"),
                    ]
                )
    in_rows     = trades.rows()
    in_rows     = [ row for row in in_rows if row[trade_row.status] == " Filled" ]
    input       = []

    for trade in in_rows:

        symbol  = trade[trade_row.product]
        scale   = 1.0

        if FUT_DEFS[symbol]["alias"]:

            scale   = FUT_DEFS[symbol]["scale"]
            symbol  = FUT_DEFS[symbol]["alias"]

        side    = 1 if trade[trade_row.b_s] == " Buy" else -1
        ts      = trade[trade_row.fill_time]
        qty     = trade[trade_row.filled_qty] * scale * side
        price   = trade[trade_row.avg_price]
        
        input.append((symbol, ts, qty, price))

    input = sorted(input, key = lambda r: r[1])

    return input