from    bisect                  import  bisect_left
from    config                  import  FUT_DEFS
from    datetime                import  datetime
from    os.path                 import  join
from    math                    import  log, sqrt
from    numpy                   import  array, cumsum, diff, mean, nonzero, std
from    numpy.random            import  choice
from    parsers                 import  ninjatrader, tradovate, thinkorswim
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_sc_df, get_spx, in_row
from    typing                  import  List


DEBUG   = 0
N       = 10_000
PARSERS = { 
            "tradovate":    tradovate,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)


# python wrc.py tydal_in America/New_York thinkorswim xxxxxx.xx 0


def get_daily(
    symbol: str,
    rows:   List, 
    tz:     str
):

    res         = pl.DataFrame()
    multiplier  = 1.
    df          = None

    if symbol in FUT_DEFS:

        multiplier = FUT_DEFS[symbol]["multiplier"]

    try:

        df = get_sc_df(symbol, tz, True).drop(
                [ 
                    "date", 
                    "time", 
                    "open",
                    "high",
                    "low",
                    " OpenInterest", 
                    " Volume"
                ]
            )
    
    except FileNotFoundError:

        print(f"{symbol}: warning, daily bars not found")

        df = pl.DataFrame({ "ts": [], "close": [] })

    trade_ts    = [ row[in_row.ts] for row in rows ]
    trade_px    = [ row[in_row.price] for row in rows ]
    trade_qty   = [ row[in_row.qty] for row in rows ]
    trades      = list(zip(trade_ts, trade_px, trade_qty))
    settles     = list(zip(df["ts"], df["close"], [ 0 for i in range(df.height) ]))
    
    start       = bisect_left(settles, rows[0][in_row.ts], key = lambda r: r[0]) - 1
    end         = bisect_left(settles, rows[-1][in_row.ts], key = lambda r: r[0]) + 1
    settles     = settles[start:end]
    combined    = sorted(trades + settles, key = lambda r: r[0])
    dates       = [ row[0].split("T")[0] for row in combined ]
    position    = array([ 0. for i in range(len(combined)) ])
    pnl         = array([ 0. for i in range(len(combined)) ])

    for i in range(len(combined)):

        position[i:] += combined[i][2]

    # correct error

    position = array([ i if abs(i) > 1e-10 else 0 for i in position ])

    for i in range(1, len(combined)):

        cur_price   = combined[i][1]
        prev_price  = combined[i - 1][1]
        prev_pos    = position[i - 1]
        pnl[i]      = (cur_price - prev_price) * prev_pos

    # convert pnls to dollars

    pnl *= multiplier

    if combined[0][2] != 0:

        print(f"{symbol}: warning, trade prior to data start")

    if combined[-1][2] != 0:

        print(f"{symbol}: warning, unclosed position at data end")

    # extra cols for debug

    res = pl.DataFrame(
                {
                    "ts":       [ row[0] for row in combined ],
                    "date":    [ date for date in dates ],
                    "price":    [ row[1] for row in combined ],
                    "qty":      [ row[2] for row in combined ],
                    "pos":      position,
                    "pnl":      pnl
                }
        )

    if DEBUG == 1:
        
        print(symbol, "\n")
        print(df.tail())
        print(res)

    if DEBUG == 2:

        print(f"{symbol:20}{sum(pnl):>10.2f}")

    res = res.group_by("date", maintain_order = True).agg(
                    [ 
                        pl.col("pnl").sum().alias("pnl"),
                    ]
                )

    return res


def bootstrap(returns: array):

    mu              = mean(returns)
    returns         = returns - mu
    sampling_dist   = sorted([ 
                        mean(
                                choice(
                                returns, 
                                size    = returns.shape[0], 
                                replace = True
                            )
                        ) 
                        for _ in range(N) 
                    ])
    i               = bisect_left(sampling_dist, mu) 
    p               = 1 - i / N

    if DEBUG == 4:

        sampling_mu = mean(sampling_dist)

        print(f"{'sampling_mu':20}{sampling_mu:>10.10f}")

        fig = go.Figure()

        fig.add_trace(go.Histogram(x = sampling_dist, name = "sampling distribution"))
        fig.add_vline(x = mu, line_color = "#FF00FF")

        fig.show()

    return p


def mc_drawdown(returns: array):

    samples   = [ 
                    choice(returns, size = returns.shape[0], replace = True)
                    for _ in range(N)
                ]
    drawdowns = sorted(
                    [
                        max(
                            [ 
                                abs(sample[i] - max(sample[0:i + 1]))
                                for i in range(len(sample)) 
                            ]
                        )
                        for sample in samples
                    ]
                )

    return drawdowns


if __name__ == "__main__":

    t0              = time()
    in_fn           = join(".", "csvs", f"{argv[1]}.csv")
    tz              = argv[2]
    parser          = PARSERS[argv[3]]
    init_balance    = float(argv[4])
    DEBUG           = int(argv[5])
    in_rows         = parser.parse(in_fn, tz, None, 0)
    symbols         = sorted(set([ row[in_row.symbol] for row in in_rows ]))
    pnls            = {}

    for symbol in symbols:

        sym_rows = [ row for row in in_rows if row[in_row.symbol] == symbol ]
        res      = get_daily(symbol, sym_rows, tz)

        if DEBUG == 3:
            
            in_rows_df = pl.DataFrame(
                {
                    "symbol":   [ row[in_row.symbol] for row in sym_rows ],
                    "ts":       [ row[in_row.ts] for row in sym_rows ],
                    "qty":      [ row[in_row.qty] for row in sym_rows ],
                    "price":    [ row[in_row.price] for row in sym_rows ]
                }
            )

            print(in_rows_df)

        for row in res.iter_rows():

            date = row[0]
            pnl  = row[1]

            if date not in pnls:

                pnls[date] = []

            pnls[date].append(pnl)

    dates       =  array(sorted(list(set([ row[in_row.ts].split("T")[0] for row in in_rows ]))))
    mask        =  array([ 1 for i in range(len(dates)) ])

    # remove saturdays and put move sunday pnls into next weekday

    for i in range(len(dates) - 1):

        cur_day = dates[i]
        dt      = datetime.strptime(dates[i], "%Y-%m-%d")

        if dt.weekday() == 5:

            mask[i] = 0
        
        if dt.weekday() == 6:

            mask[i]         =  0
            next_day        = dates[i + 1]
            pnls[next_day]  += pnls[cur_day]

    # results

    pnl                 =  [ sum(pnls[date]) for date in dates ]
    cum_pnl             =  cumsum(pnl)
    cum_pnl             += init_balance
    returns             =  array([ log(cum_pnl[i] / cum_pnl[i - 1]) for i in range(1, len(cum_pnl)) ])
    cum_ret             =  cumsum(returns)
    mu                  =  mean(returns)
    sigma               =  std(returns)
    sharpe              =  mu / sigma * sqrt(252)
    p_val               =  bootstrap(returns)
    mask                =  nonzero(mask)
    dates               =  dates[mask]
    drawdowns           =  [ cum_ret[i] - max(cum_ret[0:i + 1]) for i in range(len(cum_ret)) ]
    h_drawdowns         =  mc_drawdown(returns)
    p_95_h_dd           =  h_drawdowns[int(N * 0.95)]
    max_dd              =  min(drawdowns)
    mean_dd             =  mean(drawdowns)
    mar_ratio           =  (mu * 252) / abs(max_dd)
    gross_profit        =  sum([ i for i in pnl if i > 0 ])
    gross_loss          =  sum([ abs(i) for i in pnl if i < 0 ])
    profit_factor       =  gross_profit / gross_loss
    dates               =  dates[1:]
    SPX                 =  get_spx(dates[0], dates[-1])
    spx_dates           =  SPX["datetime"][1:]
    spx_close           =  SPX["close"]
    spx_pnl             =  diff(spx_close)
    spx_ret             =  [ log(spx_close[i] / spx_close[i - 1]) for i in range(1, len(spx_close)) ]
    spx_cum_ret         =  cumsum(spx_ret)
    spx_mu              =  mean(spx_ret)
    spx_sigma           =  std(spx_ret)
    spx_dd              =  [ spx_cum_ret[i] - max(spx_cum_ret[0:i + 1]) for i in range(len(spx_cum_ret)) ]
    spx_max_dd          =  min(spx_dd)
    spx_mean_dd         =  mean(spx_dd)
    spx_mar_ratio       =  (spx_mu * 252) / abs(spx_max_dd)
    spx_gross_profit    =  sum([ i for i in spx_pnl if i > 0 ])
    spx_gross_loss      =  sum([ abs(i) for i in spx_pnl if i < 0 ])
    spx_profit_factor   =  spx_gross_profit / spx_gross_loss
    spx_sharpe          =  spx_mu / spx_sigma * sqrt(252)

    pass

    if DEBUG == 2:

        print(f"{'TOTAL':20}{sum(pnl):>10.2f}\n")

    print("\ntotals")
    print("\n-----\n")
    print(f"{'initial balance':20}{init_balance:>10.2f}")
    print(f"{'ending balance':20}{cum_pnl[-1]:>10.2f}")
    print(f"{'pnl':20}{cum_pnl[-1] - init_balance:>10.2f}")
    print(f"{'return':20}{(cum_pnl[-1] / init_balance - 1) * 100:>10.2f}%")
    print("\n-----\n")
    print(f"{'summary statistics':20}{'trader':>10}{'spx':>10}\n")
    print(f"{'daily return:':20}{mu * 100:>10.2f}%{spx_mu * 100:>10.2f}%")
    print(f"{'daily stdev:':20}{sigma * 100:>10.2f}%{spx_sigma * 100:>10.2f}%")
    print(f"{'annualized return:':20}{mu * 252 * 100:>10.2f}%{spx_mu * 252 * 100:>10.2f}%")
    print(f"{'annualized stdev:':20}{sigma * sqrt(252) * 100:>10.2f}%{spx_sigma * sqrt(252) * 100:>10.2f}%")
    print(f"{'max drawdown:':20}{max_dd * 100:>10.2f}%{spx_max_dd * 100:>10.2f}%")
    print(f"{'avg. drawdown:':20}{mean_dd * 100:>10.2f}%{spx_mean_dd * 100:>10.2f}%")
    print(f"{'mc drawdown, p95:':20}{-p_95_h_dd * 100:>10.2f}%")
    print(f"{'MAR ratio:':20}{mar_ratio:>10.2f}{spx_mar_ratio:>10.2f}")
    print(f"{'profit factor:':20}{profit_factor:>10.2f}{spx_profit_factor:>10.2f}")
    print(f"{'sharpe ratio:':20}{sharpe:>10.2f}{spx_sharpe:>10.2f}")
    print(f"{'wrc p-value:':20}{p_val:>10.2f}")
    print("\n")

    if DEBUG == 4:

        fig = go.Figure()

        traces = [
            ( "trader", dates, cum_ret, "#0000FF" ),
            ( "spx", spx_dates, spx_cum_ret, "#FF0000" )
        ]

        for trace in traces:
        
            fig.add_trace(
                go.Scatter(
                    {
                        "x":        trace[1],
                        "y":        trace[2],
                        "name":     f"{trace[0]} returns",
                        "marker":   { "color": trace[3] }
                    }
                )
            )

        fig.show()

    print(f"{time() - t0:0.1f}s")

    pass