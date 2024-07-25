from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    numpy                   import  array, cumsum
from    os.path                 import  join
from    polars                  import  DataFrame, col, Config, Datetime, read_csv
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
    trades      = trades.with_columns(
                    [
                        col("boughtTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("boughtTimestamp"),
                        col("soldTimestamp").str.strptime(Datetime, TV_DT_FMT).dt.strftime(DBN_DT_FMT).alias("soldTimestamp")
                    ]
                )
    symbols     = [ sym[:-2] for sym in list(trades["symbol"].unique()) ]
    input       = []
    output      = []
    sym_data    = get_sym_data(symbols, schema, DBN_DT_FMT, tz)
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
        #sym_px      = (sym_data[symbol]["open"] + sym_data[symbol]["high"] + sym_data[symbol]["low"] + sym_data[symbol]["close"]) / 4
        
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
    
    if output:

        output  = sorted(output, key = lambda r: r[1])
        input   = sorted(input, key = lambda r: r[1])

        with open(out_fn, "w") as fd:

            fd.write("symbol,ts,idx,pos_chg\n")

            for line in output:

                fd.write(",".join([ str(i) for i in line[:-1] ]) + "\n")

    if debug:

        # print(hist)

        for symbol in symbols:

            in_rows         = [ row for row in input if row[0] == symbol ]
            in_position     = in_rows[0][3]
            in_pnl          = 0.
            in_pnls         = [ in_pnl ]
            
            out_rows        = [ row for row in output if row[0] == symbol ]
            out_position    = out_rows[0][3]
            out_pnl         = 0.
            out_pnls        = [ out_pnl ]
            
            for i in range(1, len(in_rows)):

                in_pnl          += in_position * (in_rows[i][4] - in_rows[i - 1][4])
                in_position     += in_rows[i][3]
                out_pnl         += out_position * (out_rows[i][4] - out_rows[i - 1][4])
                out_position    += out_rows[i][3]
                
                in_pnls.append(in_pnl)
                out_pnls.append(out_pnl)

            in_pnls         = array(in_pnls)
            out_pnls        = array(out_pnls)
            in_px           = array([ row[4] for row in in_rows ])
            out_px          = array([ row[4] for row in out_rows ])
            diff_px         = out_px - in_px
            diff_pnl        = out_pnls - in_pnls
            diff_pnl_pct    = (out_pnls / in_pnls - 1) * 100

            df = DataFrame(
                {
                    "symbol":       [ row[0] for row in out_rows ],
                    "in_ts":        [ row[1] for row in in_rows ],
                    "out_ts":       [ row[1] for row in out_rows ],
                    "in_qty":       [ row[3] for row in in_rows ],
                    "out_qty":      [ row[3] for row in out_rows ],
                    "in_px":        in_px,
                    "out_px":       out_px,
                    "diff_px":      diff_px,
                    "in_pnl":       in_pnls,
                    "out_pnl":      out_pnls,
                    "diff_pnl":     diff_pnl,
                    "diff_pnl (%)": diff_pnl_pct
                }
            )

            print(df)