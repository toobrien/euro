from    datetime                import  datetime
from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    os.path                 import  join
from    numpy                   import  array, cumsum, diff, zeros
from    polars                  import  col, Config, Datetime, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    util                    import  adjust_tz, get_sym_data


# python get_history.py 20240709_in ohlcv-1s Europe/Berlin 1


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
DBN_DT_FMT  = "%Y-%m-%dT%H:%M:%S"

Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    in_fn       = join(".", "csvs", f"{argv[1]}.csv")
    out_fn      = join(".", "csvs", f"{argv[1][:-3]}_out.csv")
    schema      = argv[2]
    tz          = argv[3]
    debug       = int(argv[4])
    trades      = read_csv(in_fn)

    if debug:

        print(trades.select([ "symbol", "qty", "boughtTimestamp", "buyPrice", "soldTimestamp", "sellPrice", "pnl" ]))

    trades      = trades.with_columns(
                    [
                        col("boughtTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("boughtTimestamp"),
                        col("soldTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("soldTimestamp")
                    ]
                )
    symbols     = [ sym[:-2] for sym in list(trades["symbol"].unique()) ]
    history     = []
    sym_data    = get_sym_data(symbols, schema, DBN_DT_FMT, tz)
    trades      = trades.rows()

    for trade in trades:

        symbol      = trade[trade_row.symbol][:-2]
        buy_ts      = trade[trade_row.boughtTimestamp]
        buy_chg     = trade[trade_row.qty]
        sell_ts     = trade[trade_row.soldTimestamp]
        sell_chg    = -buy_chg
        sym_ts      = sym_data[symbol]["ts"]
        
        if buy_ts < sym_ts[0] or sell_ts > sym_ts[-1]:
        
            # no data for trade

            continue
        
        buy_idx     = bisect_left(sym_ts, buy_ts)
        sell_idx    = bisect_left(sym_ts, sell_ts)

        history.append((symbol, buy_ts, buy_idx, buy_chg))
        history.append((symbol, sell_ts, sell_idx, sell_chg))

    if history:

        history = sorted(history, key = lambda r: r[1])

        with open(out_fn, "w") as fd:

            fd.write("symbol,ts,idx,pos_chg\n")

            for line in history:

                fd.write(",".join([ str(i) for i in line ]) + "\n")

    if debug:

        hist = read_csv(out_fn)

        # print(hist)

        for symbol in symbols:

            rows        = hist.filter(col("symbol") == symbol).rows()
            start       = rows[0][-2]
            stop        = rows[-1][-2]
            closes      = array(sym_data[symbol]["close"])
            chgs        = diff(closes)
            position    = zeros(len(chgs))
            
            print(f"{'o_ts':20}{'o_px':10}{'o_pos_chg':10}{'c_ts':20}{'c_px':10}{'c_pos_chg':10}\n")

            for row in rows:

                position[row[-2] - 1:]+= row[-1]

            for i in range(0, len(rows), 2):

                o       = rows[i]
                c       = rows[i + 1]
                o_txt   = f"{o[1]:20}{closes[o[-2]]:>10}{o[-1]:>10}"
                c_txt   = f"{c[1]:20}{closes[c[-2]]:>10}{c[-1]:>10}"

                print(f"{o_txt}\t{c_txt}")

            pnl = cumsum(chgs * position)
            act = [ r[trade_row.pnl] for r in trades ]
            act = [ float(pnl[1:]) if "(" not in pnl else -float(pnl[2:-1]) for pnl in act ]
            act = cumsum(act)
            qty = cumsum( [ r[trade_row.qty] for r in trades ])

            print("\n")
            print(f"{symbol} pnl (pt):  {pnl[-1]}")
            print(f"{symbol} act  ($):  {act[-1]}")
            print(f"{symbol} cons (rt): {qty[-1]}")

            pass