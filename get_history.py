from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    numpy                   import  cumsum
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
    history     = []
    master      = []
    sym_data    = get_sym_data(symbols, schema, DBN_DT_FMT, tz)
    in_rows     = trades.rows()

    for trade in in_rows:

        symbol      = trade[trade_row.symbol][:-2]
        m_buy_ts    = trade[trade_row.boughtTimestamp]
        m_sell_ts   = trade[trade_row.soldTimestamp]
        m_buy_chg   = trade[trade_row.qty]
        m_sell_chg  = -m_buy_chg
        m_buy_px    = trade[trade_row.buyPrice]
        m_sell_px   = trade[trade_row.sellPrice]
        sym_ts      = sym_data[symbol]["ts"]
        sym_px      = sym_data[symbol]["open"]
        
        if m_buy_ts < sym_ts[0] or m_sell_ts > sym_ts[-1]:
        
            # no data for trade

            continue
        
        h_buy_idx   = bisect_left(sym_ts, m_buy_ts)
        h_sell_idx  = bisect_left(sym_ts, m_sell_ts)
        h_buy_ts    = sym_ts[h_buy_idx]
        h_sell_ts   = sym_ts[h_sell_idx]
        h_buy_px    = sym_px[h_buy_idx]
        h_sell_px   = sym_px[h_sell_idx]

        history.append((symbol, h_buy_ts, h_buy_idx, m_buy_chg, h_buy_px))
        history.append((symbol, h_sell_ts, h_sell_idx, m_sell_chg, h_sell_px))
        master.append((symbol, m_buy_ts, None, m_buy_chg, m_buy_px))
        master.append((symbol, m_sell_ts, None, m_sell_chg, m_sell_px))

    if history:

        master  = sorted(master, key = lambda r: r[1])
        history = sorted(history, key = lambda r: r[1])

        with open(out_fn, "w") as fd:

            fd.write("symbol,ts,idx,pos_chg\n")

            for line in history:

                fd.write(",".join([ str(i) for i in line[:-1] ]) + "\n")

    if debug:

        # print(hist)

        for symbol in symbols:

            h_rows  = [ row for row in history if row[0] == symbol ]
            m_rows  = [ row for row in master if row[0] == symbol ]

            df_seq  = DataFrame(
                {
                    "symbol":   [ row[0] for row in h_rows ],
                    "in_ts":    [ row[1] for row in m_rows ],
                    "in_qty":   [ row[3] for row in m_rows ],
                    "in_px":    [ row[4] for row in m_rows ],
                    "out_ts":   [ row[1] for row in h_rows ],
                    "out_qty":  [ row[3] for row in h_rows ],
                    "out_px":   [ row[4] for row in h_rows ]
                }
            )
            
            out_open_px     = [ h_rows[i][4] for i in range(0, len(h_rows), 2) ]
            out_close_px    = [ h_rows[i][4] for i in range(1, len(h_rows), 2) ]
            out_qty         = [ h_rows[i][3] for i in range(0, len(h_rows), 2) ]
            out_pnl         = [ (out_close_px[i] - out_open_px[i]) * out_qty[i] for i in range(len(out_open_px)) ]
            in_open_px      = [ m_rows[i][4] for i in range(0, len(m_rows), 2) ]
            in_close_px     = [ m_rows[i][4] for i in range(1, len(m_rows), 2) ]
            in_qty          = [ m_rows[i][3] for i in range(0, len(m_rows), 2) ]
            in_pnl          = [ (in_close_px[i] - in_open_px[i]) * in_qty[i] for i in range(len(in_open_px)) ]
            diff_pnl        = [ out_pnl[i] - in_pnl[i] for i in range(len(in_pnl)) ]

            df_pnl = DataFrame(
                {
                    "trade":        [ i for i in range(int(len(h_rows) / 2)) ],
                    "in_open_px":   in_open_px,
                    "in_close_px":  in_close_px,
                    "in_qty":       in_qty,
                    "in_pnl":       in_pnl,
                    "out_open_px":  out_open_px,
                    "out_close_px": out_close_px,
                    "out_qty":      out_qty,
                    "out_pnl":      out_pnl,
                    "diff_pnl":     diff_pnl

                }
            )
            
            df_pnl = df_pnl.with_columns((col("diff_pnl").cum_sum()).alias("diff_pnl_cum"))

            print(df_seq)
            print(df_pnl)

            total_in_pnl    = sum(in_pnl)
            total_out_pnl   = sum(out_pnl)
            diff            = total_out_pnl - total_in_pnl

            print(f"input pnl:  {total_in_pnl}")
            print(f"output pnl: {total_out_pnl}")
            print(f"diff:       {diff}")