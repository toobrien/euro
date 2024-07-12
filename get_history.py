from    datetime                import  datetime
from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    os.path                 import  join
from    polars                  import  col, Config, Datetime, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    util                    import  adjust_tz


# python get_history.py 20240709_perf ohlcv-1s Europe/Berlin 1


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
SYM_MAP     = {}

Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    fn      = argv[1]
    schema  = argv[2]
    tz      = argv[3]
    debug   = int(argv[4])
    trades  = read_csv(join(".", "csvs", f"{fn}.csv"))
    trades  = trades.with_columns(
                [
                    col("boughtTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("boughtTimestamp"),
                    col("soldTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("soldTimestamp")
                ]
            )
    symbols = [ sym[:-2] for sym in list(trades["symbol"].unique()) ]

    for symbol in symbols:

        ohlcv       = read_csv(f"../databento/csvs/{symbol}.c.0_{schema}.csv")
        ohlcv       = adjust_tz(ohlcv, "ts_event", "ts_event", DBN_DT_FMT, tz)
        ts          = [ ts_.split(".")[0] for ts_ in list(ohlcv["ts_event"]) ]
        position    = [ 0 for _ in ts ]

        SYM_MAP[symbol] = {
            "ts":       ts,
            "pos":      position,
            "close":    list(ohlcv["close"]),
            "start":    None,
            "end":      None
        }
    
    trades = trades.rows()

    for trade in trades:

        symbol      = trade[trade_row.symbol][:-2]
        multiplier  = 1 if trade[trade_row.buyFillId] < trade[trade_row.sellFillId] else -1
        qty         = trade[trade_row.qty] * multiplier
        i_ts, j_ts  = sorted([ 
                        trade[trade_row.boughtTimestamp],
                        trade[trade_row.soldTimestamp]
                    ])
        sym_ts      = SYM_MAP[symbol]["ts"]
        sym_pos     = SYM_MAP[symbol]["pos"]

        i           = bisect_left(sym_ts, i_ts)
        j           = bisect_left(sym_ts, j_ts)

        # truncate excess data

        if not SYM_MAP[symbol]["start"]:

            SYM_MAP[symbol]["start"] = i - 3600
        
        if trade == trades[-1]:

            SYM_MAP[symbol]["end"] = j + 3600

        if i not in range(len(sym_pos)) or j not in range(len(sym_pos)):

            # no data for trade

            break

        for k in range(i, j + 1):

            sym_pos[k] += qty

        #print(i_ts, sym_ts[i])
        #print(j_ts, sym_ts[j])

    if debug:

        for sym, data in SYM_MAP.items():

            i = data["start"]
            j = data["end"]

            fig = go.Figure()

            X = data["ts"][i:j]
            Y = data["close"][i:j]

            fig.add_trace(
                go.Scattergl(
                    {
                        "x": X,
                        "y": Y
                    }
                )
            )

            pos     = data["pos"][i:j]
            arrow   = {
                "x":            None,
                "y":            None,
                "showarrow":    True,
                "arrowhead":    3,
                "arrowwidth":   1.5
            } 

            for i in range(1, len(pos)):

                cur_pos     = pos[i]
                prev_pos    = pos[i - 1]

                if cur_pos > prev_pos:

                    arrow["text"]       = f"+{cur_pos - prev_pos}"
                    arrow["font"]       = { "color": "#0000FF" }
                    arrow["x"]          = X[i]
                    arrow["y"]          = Y[i]
                    arrow["arrowcolor"] = "#0000FF"

                    fig.add_annotation(**arrow)

                elif cur_pos < prev_pos:

                    arrow["text"]       = f"-{cur_pos - prev_pos}"
                    arrow["font"]       = { "color": "#FF0000" }
                    arrow["x"]          = X[i]
                    arrow["y"]          = Y[i]
                    arrow["arrowcolor"] = "#FF0000"

                    fig.add_annotation(**arrow)

            fig.show()

    pass