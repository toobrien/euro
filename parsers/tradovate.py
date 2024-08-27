from config     import TS_FMT, FUT_DEFS
from enum       import IntEnum
from polars     import col, Datetime, read_csv


class trade_row(IntEnum):

    symbol              = 0
    _priceFormat        = 1
    _priceFormatType    = 2
    _tickSize           = 3
    buyFillId           = 4
    sellFillId          = 5
    qty                 = 6
    buyPrice            = 7
    sellPrice           = 8
    pnl                 = 9
    boughtTimestamp     = 10
    soldTimestamp       = 11
    duration            = 12


TV_DT_FMT = "%m/%d/%Y %H:%M:%S" 


def parse(in_fn: str):

    trades      = read_csv(in_fn)
    trades      = trades.with_columns(
                    [
                        col("boughtTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(TS_FMT).alias("boughtTimestamp"),
                        col("soldTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(TS_FMT).alias("soldTimestamp")
                    ]
                )
    in_rows     = trades.rows()
    input       = []

    for trade in in_rows:

        symbol  = trade[trade_row.symbol][:-2]
        scale   = 1.0

        if FUT_DEFS[symbol]["alias"]:

            scale   = FUT_DEFS[symbol]["scale"]
            symbol  = FUT_DEFS[symbol]["alias"]

        in_buy_ts   = trade[trade_row.boughtTimestamp]
        in_sell_ts  = trade[trade_row.soldTimestamp]
        in_qty      = trade[trade_row.qty] * scale
        in_buy_px   = trade[trade_row.buyPrice]
        in_sell_px  = trade[trade_row.sellPrice]
        
        input.append((symbol, in_buy_ts, in_qty, in_buy_px))
        input.append((symbol, in_sell_ts, -in_qty, in_sell_px))
    
    input = sorted([ row for row in input if row[0] ], key = lambda r: r[1])

    return input