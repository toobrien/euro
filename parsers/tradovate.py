from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    polars                  import  col, Datetime, read_csv
from    sys                     import  path

path.append(".")

from    util                    import  adjust_tz, get_sym_data


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


TV_DT_FMT   = "%m/%d/%Y %H:%M:%S" 


def parse(
    in_fn:      str,
    schema:     str,
    tz:         str,
    out_dt_fmt: str
):

    trades      = read_csv(in_fn)
    trades      = trades.with_columns(
                    [
                        col("boughtTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(out_dt_fmt).alias("boughtTimestamp"),
                        col("soldTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(out_dt_fmt).alias("soldTimestamp")
                    ]
                )
    symbols     = [ sym[:-2] for sym in list(trades["symbol"].unique()) ]
    input       = []
    output      = []
    sym_data    = get_sym_data(symbols, schema, out_dt_fmt, tz)
    in_rows     = trades.rows()

    for trade in in_rows:

        symbol      = trade[trade_row.symbol][:-2]
        in_buy_ts   = trade[trade_row.boughtTimestamp]
        in_sell_ts  = trade[trade_row.soldTimestamp]
        in_qty      = trade[trade_row.qty]
        in_buy_px   = trade[trade_row.buyPrice]
        in_sell_px  = trade[trade_row.sellPrice]
        sym_ts      = sym_data[symbol]["ts"]
        sym_px      = sym_data[symbol]["open"]
        
        if in_buy_ts < sym_ts[0] or in_sell_ts > sym_ts[-1]:
        
            # no data for trade

            continue
        
        out_buy_idx     = bisect_left(sym_ts, in_buy_ts)
        out_sell_idx    = bisect_left(sym_ts, in_sell_ts)
        out_buy_ts      = sym_ts[out_buy_idx]
        out_sell_ts     = sym_ts[out_sell_idx]
        out_buy_px      = sym_px[out_buy_idx]
        out_sell_px     = sym_px[out_sell_idx]

        input.append((symbol, in_buy_ts, None, in_qty, in_buy_px))
        input.append((symbol, in_sell_ts, None, -in_qty, in_sell_px))
        output.append((symbol, out_buy_ts, out_buy_idx, in_qty, out_buy_px))
        output.append((symbol, out_sell_ts, out_sell_idx, -in_qty, out_sell_px))

    return symbols, input, output