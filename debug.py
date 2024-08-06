from    datetime                import  datetime, timedelta
from    numpy                   import  array, cumsum
from    math                    import  log
from    os.path                 import  join
import  plotly.graph_objects    as      go
from    polars                  import  col, Config, DataFrame, read_csv
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from util       import get_sym_data


Config.set_tbl_rows(-1)
Config.set_tbl_cols(-1)


# python debug.py reset_pa sc Europe/Berlin


if __name__ == "__main__":

    t0          = time()
    debug_fn    = join(".", "csvs", f"{argv[1]}_debug.csv")
    out_fn      = join(".", "csvs", f"{argv[1]}_out.csv")
    src         = argv[2]
    tz          = argv[3]
    input       = read_csv(debug_fn).rows()
    output      = read_csv(out_fn).rows()
    symbols     = set([ row[0] for row in input ])
    start       = input[0][1].split("T")[0]
    end         = (datetime.strptime(input[-1][1].split("T")[0], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    sym_data    = get_sym_data(symbols, start, end, tz, src)

    for symbol in symbols:

        in_rows         = [ row for row in input if row[0] == symbol ]
        in_position     = in_rows[0][2]
        in_pnl          = 0.
        in_pnls         = [ in_pnl ]
        in_ret          = 0.
        in_rets         = [ in_ret ]
        out_rows        = [ row for row in output if row[0] == symbol ]
        out_position    = out_rows[0][3]
        out_pnl         = 0.
        out_pnls        = [ out_pnl ]
        out_ret         = 0.
        out_rets        = [ out_ret ]
        in_dates        = [ row[1].split("T")[0] for row in input ]
        
        for i in range(1, len(in_rows)):

            in_pnl          =   in_position * (in_rows[i][3] - in_rows[i - 1][3])
            in_ret          =   in_position * log(in_rows[i][3] / in_rows[i - 1][3])
            in_position     +=  in_rows[i][2]
            out_pnl         =   out_position * (out_rows[i][4] - out_rows[i - 1][4])
            out_ret         =   out_position * log(out_rows[i][4] / out_rows[i - 1][4])
            out_position    +=  out_rows[i][3]

            in_pnls.append(in_pnl)
            in_rets.append(in_ret)
            out_pnls.append(out_pnl)
            out_rets.append(out_ret)

        in_pnls         = array(in_pnls)
        out_pnls        = array(out_pnls)
        in_px           = array([ row[3] for row in in_rows ])
        out_px          = array([ row[4] for row in out_rows ])
        diff_px         = out_px - in_px
        diff_pnl        = out_pnls - in_pnls

        df = DataFrame(
                {
                    "trade":        [ i for i in range(len(in_rows)) ],
                    "symbol":       [ row[0] for row in out_rows ],
                    "in_ts":        [ row[1] for row in in_rows ],
                    "out_ts":       [ row[1] for row in out_rows ],
                    "in_qty":       [ row[2] for row in in_rows ],
                    "out_qty":      [ row[3] for row in out_rows ],
                    "in_px":        in_px,
                    "out_px":       out_px,
                    "diff_px":      diff_px,
                    "in_pnl":       in_pnls,
                    "out_pnl":      out_pnls,
                    "diff_pnl":     diff_pnl,
                    "in_pnl_cum":   cumsum(in_pnls),
                    "out_pnl_cum":  cumsum(out_pnls),
                    "diff_pnl_cum": cumsum(diff_pnl)
                    
                }
            )
        
        df = df.with_columns((col("out_pnl_cum") / col("in_pnl_cum") - 1).alias("diff_pnl_cum_pct"))

        print("\ntrade, cumulative\n")

        print(df)

        # daily returns

        print("\ndaily\n")
        
        day_df  = DataFrame(
                    { 
                        "date":     in_dates, 
                        "in_pnl":   in_pnls, 
                        "out_pnl":  out_pnls,
                        "diff_pnl": diff_pnl,
                        "in_ret":   in_rets,  
                        "out_ret":  out_rets
                    }
                )
        day_df      = day_df.group_by(
                        "date", 
                        maintain_order = True
                    ).agg(
                        [ 
                            col("in_pnl").sum(),
                            col("out_pnl").sum(),
                            col("diff_pnl").sum(),
                            col("in_ret").sum(),
                            col("out_ret").sum()
                        ]
                    )
        
        print(day_df)
        
        print(f"\n{'':10}{'pnl':>10}{'ret':>10}")
        print(f"{'in:':10}{day_df['in_pnl'].sum():>10.2f}{day_df['in_ret'].sum() * 100:>9.2f}%")
        print(f"{'out:':10}{day_df['out_pnl'].sum():>10.2f}{day_df['out_ret'].sum() * 100:>9.2f}%\n")

        # error plot

        fig         = go.Figure()
        opens       = sym_data[symbol]["open"]
        highs       = sym_data[symbol]["high"]
        lows        = sym_data[symbol]["low"]
        closes      = sym_data[symbol]["close"]
        idxs        = [ row[2] for row in output ]
        X           = [ row[1] for row in in_rows ]
        o_trace     = []
        h_trace     = []
        l_trace     = []
        c_trace     = []
        a_trace     = []
        text        = []

        for i in range(len(in_px)):

            in_price    = in_px[i]
            idx         = idxs[i]
            o_          = opens[idx] - in_price
            h_          = highs[idx] - in_price
            l_          = lows[idx] - in_price
            c_          = closes[idx] - in_price

            o_trace.append(o_)
            h_trace.append(h_)
            l_trace.append(l_)
            c_trace.append(c_)
            a_trace.append((o_ + c_) / 2)
            text.append(i)

        traces = [
            ( "open",   o_trace,    "#00FFFF" ),
            ( "high",   h_trace,    "#0000FF" ),
            ( "low",    l_trace,    "#FF0000" ),
            ( "close",  c_trace,    "#FF00FF" ),
            ( "avg",    a_trace,    "#CCCCCC" ),
            ( "d_pnl",  diff_pnl,   "#000000" )
        ]

        print("\nerrors:\n")

        for trace in traces:

            fig.add_trace(
                go.Scatter(
                    {
                        "x":        X,
                        "y":        trace[1],
                        "name":     trace[0],
                        "mode":     "markers",
                        "marker":   { "color": trace[2] },
                        "text":     text

                    }
                )
            )

            print(f"{trace[0]:10}{sum(trace[1]):>10.2f}")

        fig.show()
    
    print(f"\n{time() - t0:0.1f}s\n")

    pass