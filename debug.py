from    datetime                import  datetime, timedelta
from    numpy                   import  array, cumsum, mean
from    math                    import  log
from    os.path                 import  join
import  plotly.graph_objects    as      go
from    plotly.subplots         import  make_subplots
from    polars                  import  col, Config, DataFrame, read_csv
from    random                  import  randint
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from util       import get_sym_data, in_row, out_row


Config.set_tbl_rows(-1)
Config.set_tbl_cols(-1)


SHOW_TRADE_DF       = False
SHOW_DAILY_RETURNS  = True
SHOW_PNL_HIST       = False
SHOW_OUTLIERS       = False
SHOW_ERRS           = True
SHOW_ERR_PLOT       = True


# python debug.py reset_pa sc Europe/Berlin


if __name__ == "__main__":

    t0          = time()
    debug_fn    = join(".", "csvs", f"{argv[1]}_debug.csv")
    out_fn      = join(".", "csvs", f"{argv[1]}_out.csv")
    src         = argv[2]
    tz          = argv[3]
    input       = read_csv(debug_fn).rows()
    output      = read_csv(out_fn).rows()
    symbols     = set([ row[in_row.symbol] for row in input ])
    start       = input[0][in_row.ts].split("T")[0]
    end         = (datetime.strptime(input[-1][in_row.ts].split("T")[0], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    sym_data    = get_sym_data(symbols, start, end, tz, src)

    for symbol in symbols:

        opens           = sym_data[symbol]["open"]
        highs           = sym_data[symbol]["high"]
        lows            = sym_data[symbol]["low"]
        closes          = sym_data[symbol]["close"]
        in_rows         = [ row for row in input if row[in_row.symbol] == symbol ]
        in_position     = in_rows[0][in_row.qty]
        in_pnl          = 0.
        in_pnls         = [ in_pnl ]
        in_ret          = 0.
        in_rets         = [ in_ret ]
        out_rows        = [ row for row in output if row[out_row.symbol] == symbol ]
        out_position    = out_rows[0][out_row.pos_chg]
        out_pnl         = 0.
        out_pnls        = [ out_pnl ]
        out_ret         = 0.
        out_rets        = [ out_ret ]
        in_dates        = [ row[in_row.ts].split("T")[0] for row in input ]
        
        for i in range(1, len(in_rows)):

            in_pnl          =   in_position * (in_rows[i][in_row.price] - in_rows[i - 1][in_row.price])
            in_ret          =   in_position * log(in_rows[i][in_row.price] / in_rows[i - 1][in_row.price])
            in_position     +=  in_rows[i][2]
            out_pnl         =   out_position * (out_rows[i][out_row.out_price] - out_rows[i - 1][out_row.out_price])
            out_ret         =   out_position * log(out_rows[i][out_row.out_price] / out_rows[i - 1][out_row.out_price])
            out_position    +=  out_rows[i][3]

            in_pnls.append(in_pnl)
            in_rets.append(in_ret)
            out_pnls.append(out_pnl)
            out_rets.append(out_ret)

        in_pnls         = array(in_pnls)
        out_pnls        = array(out_pnls)
        in_px           = array([ row[in_row.price] for row in in_rows ])
        out_px          = array([ row[out_row.out_price] for row in out_rows ])
        diff_px         = out_px - in_px
        diff_pnl        = out_pnls - in_pnls

        if SHOW_TRADE_DF:

            df = DataFrame(
                    {
                        "trade":        [ i for i in range(len(in_rows)) ],
                        "symbol":       [ row[out_row.symbol] for row in out_rows ],
                        "in_ts":        [ row[in_row.ts] for row in in_rows ],
                        "out_ts":       [ row[out_row.ts] for row in out_rows ],
                        "in_qty":       [ row[in_row.qty] for row in in_rows ],
                        "out_qty":      [ row[out_row.pos_chg] for row in out_rows ],
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

        if SHOW_DAILY_RETURNS:

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
            
            print(f"\n{'':15}{'pnl':>10}{'ret':>10}")
            print(f"{'in:':15}{day_df['in_pnl'].sum():>10.2f}{day_df['in_ret'].sum() * 100:>9.2f}%")
            print(f"{'out:':15}{day_df['out_pnl'].sum():>10.2f}{day_df['out_ret'].sum() * 100:>9.2f}%\n")

        # error plot

        if SHOW_ERRS:

            fig         = go.Figure()
            idxs        = [ row[out_row.idx] for row in output ]
            X           = [ i for i in range(len(in_px)) ]
            text        = [ row[in_row.ts] for row in in_rows ]
            o_trace     = []
            h_trace     = []
            l_trace     = []
            c_trace     = []
            a_trace     = []

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

            traces = [
                #( "open",   o_trace,    "#00FFFF" ),
                #( "high",   h_trace,    "#0000FF" ),
                #( "low",    l_trace,    "#FF0000" ),
                ( "close",  c_trace,    "#FF00FF" ),
                #( "avg",    a_trace,    "#CCCCCC" ),
                ( "pnl",    diff_pnl,   "#000000" )
            ]

            print("\nerrors:\n")

            print(f"{'':15}{'total':>10}{'pos_pct':>10}{'pos_mean':>10}{'neg_pct':>10}{'neg_mean':>10}\n")

            if SHOW_ERR_PLOT:

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

                    pos_errs = [ i for i in trace[1] if i > 0 ]
                    pos_pct  = len(pos_errs) / len(trace[1])
                    pos_mean = mean(pos_errs)
                    neg_errs = [ i for i in trace[1] if i < 0 ]
                    neg_pct  = len(neg_errs) / len(trace[1])
                    neg_mean = mean(neg_errs)

                    print(f"{trace[0]:15}{sum(trace[1]):>10.2f}{pos_pct:>10.2f}{pos_mean:>10.2f}{neg_pct:>10.2f}{neg_mean:>10.2f}")

                fig.add_hline(y = 0, line_color = "#FF0000")

                fig.show()

                # "positions" is only valid for non-overlapping trades
                
                positions   = [ in_rows[i][in_row.qty] for i in range(0, len(in_rows), 2) ]
                p_long      = mean([ 1 if val > 0 else 0 for val in positions ])
                p_short     = 1 - p_long
                
                print("\n")
                print(f"{'long:':15}{p_long:>10.2f}")
                print(f"{'short:':15}{p_short:>10.2f}\n")

        
        # pnl err hist

        if SHOW_PNL_HIST:

            fig = go.Figure()

            fig.add_trace(go.Histogram(x = diff_pnl, name = "diff_pnl"))

            fig.show()

        # outlier plots

        if SHOW_OUTLIERS:

            # output:   symbol,ts,idx,pos_chg,price

            N           = 50
            n           = randint(0, N - 1)
            buffer      = 10
            width       = 1200
            margin      = 20
            height      = (400 + 2 * margin) * N
            outliers    = sorted(
                                [ 
                                    ( 
                                        i,                  # 0: trade index
                                        in_px[i - 1],       # 1: prev in price
                                        in_px[i],           # 2: cur in price
                                        out_px[i - 1],      # 3: prev out price
                                        out_px[i],          # 4: cur out price
                                        out_rows[i - 1][2], # 5: prev idx
                                        out_rows[i][2],     # 6: cur idx
                                        diff_pnl[i]         # 7: error
                                    ) 
                                    for i in range(1, len(diff_pnl))
                                ],
                                key = lambda r: abs(r[-1])
                            )[-N:]
            selected    = outliers[n]
            #fig        = make_subplots(rows = N, cols = 1, vertical_spacing = 0.02)
            fig         = go.Figure()

            '''
            fig.update_layout(
                {
                    "width":    width,
                    "height":   height,
                    "margin":   {
                        "b":   margin,
                        "t":      margin
                    }
                }
            )
            '''

            #for n in range(N):

            outlier = outliers[n]
            i       = outlier[5]
            j       = outlier[6]
            i_      = i - buffer
            j_      = j + buffer
            o_      = opens[i_:j_]
            h_      = highs[i_:j_]
            l_      = lows[i_:j_]
            c_      = closes[i_:j_]

            fig.add_trace(
                go.Candlestick(
                    {
                        "open":     o_,
                        "high":     h_,
                        "low":      l_,
                        "close":    c_,
                        "name":     f"trade {outlier[0]}, diff {outlier[-1]:0.2f}"
                    }
                )
                #row = n + 1,
                #col = 1
            )

            anns = [ 
                    ( outlier[1], "in", buffer ),
                    ( outlier[2], "in", j - i + buffer ),
                    ( outlier[3], "out", buffer ),
                    ( outlier[4], "out", j - i + buffer )
                ]

            for ann in anns:

                fig.add_annotation(
                    x       = ann[2],
                    y       = ann[0],
                    text    = f"{ann[0]:0.2f} {ann[1]}"
                    #row     = n + 1,
                    #col     = 1
                )

            fig.update_layout(xaxis_rangeslider_visible = False)
            fig.show()

    print(f"{time() - t0:0.1f}s\n")