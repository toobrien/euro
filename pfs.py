from    arch.bootstrap          import  IIDBootstrap
from    bisect                  import  bisect_left
from    config                  import  FUT_DEFS
from    os.path                 import  join
from    math                    import  log
from    numpy                   import  arange, array, corrcoef, cumsum, diag, diff, mean, std, sqrt, vstack
from    numpy.random            import  choice
from    parsers                 import  ninjatrader, tradovate, tradovate_tv, thinkorswim
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sklearn.linear_model    import  LinearRegression
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_sc_df, get_spx, in_row
from    typing                  import  List


SKIP    = [ "GC" ]
DEBUG   = 0
N       = 10_000
PARSERS = { 
            "tradovate":    tradovate,
            "tradovate_tv": tradovate_tv,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)


# python pfs.py tydal_in America/New_York thinkorswim xxxxxx.xx 0


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

        print(f"{symbol:20}{sum(pnl):>15.2f}")

    res = res.group_by("date", maintain_order = True).agg(
                    [ 
                        pl.col("pnl").sum().alias("pnl"),
                    ]
                )

    return res


def mean_bootstrap(returns: array):

    mu              = mean(returns)
    returns         = returns - mu
    sampling_dist   = sorted([ 
                        mean(choice(returns, size = returns.shape[0], replace = True)) 
                        for _ in range(N) 
                    ])
    i               = bisect_left(sampling_dist, mu) 
    p               = 1 - i / N

    if DEBUG == 4:

        sampling_mu = mean(sampling_dist)

        print(f"{'sampling_mu':20}{sampling_mu:>15.15f}")

        fig = go.Figure()

        fig.add_trace(go.Histogram(x = sampling_dist, name = "sampling distribution"))
        fig.add_vline(x = mu, line_color = "#FF00FF")

        fig.show()

    return p


def sharpe_bootstrap(returns: array):
    
    alpha           = 0.95
    beta            = 1 - alpha
    M               = returns.shape[0]
    sample_mu       = mean(returns)
    sample_sigma    = std(returns)
    sample_sharpe   = sample_mu / sample_sigma
    sample_std_err  = sqrt((1 + 1/2 * sample_sharpe**2) / M)
    sampling_dist   = []

    for _ in range(N):

        resample            = choice(returns, size = M, replace = True)
        resample_mu         = mean(resample)
        resample_sigma      = std(resample)
        resample_sharpe     = resample_mu / resample_sigma
        resample_std_err    = sqrt((1 + 1/2 * resample_sharpe**2) / M)
        T                   = (resample_sharpe - sample_sharpe) / resample_std_err

        sampling_dist.append(T)
    
    sampling_dist   = sorted(sampling_dist)
    sampling_dist   = array([ sample_sharpe + (sample_std_err * t) for t in sampling_dist ]) * sqrt(252)
    i               = int(beta / 2 * N)
    j               = int((1 - beta / 2) * N)
    index_sr        = 0.48
    res             = ( sampling_dist[i], sampling_dist[j], p )
    p               = bisect_left(sampling_dist, index_sr) / N

    '''
    # reference -- 95% CI

    def sr(x):

        mu      = mean(x)
        sigma   = std(x)
        
        return array([ mu, sigma, mu / sigma ])
    
    bs = IIDBootstrap(returns)
    ci = bs.conf_int(sr, N, method = "percentile")
    ci = ci * sqrt(252)
    '''

    return res


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
    in_rows         = [ row for row in in_rows if row[in_row.symbol] not in SKIP ]
    start           = in_rows[0][in_row.ts].split("T")[0]
    end             = in_rows[-1][in_row.ts].split("T")[0]
    symbols         = sorted(set([ row[in_row.symbol] for row in in_rows ]))
    SPX             = get_spx(start, end)

    # get_spx() returns one additional day prior to the first trade date, 
    # for calculating SPX return on the first day.
    # dates are only used for labeling returns, so trim spx_dates[0]

    dates           = list(SPX["datetime"])[1:]
    spx_close       = SPX["close"]
    pnls            = { date: [] for date in dates }

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

            date    = row[0]
            pnl     = row[1]

            # for weekend/holiday trades, the pnl will be appended to the next market day

            i       = bisect_left(dates, date)
            date    = dates[i]

            pnls[date].append(pnl)
    
    # index statistics

    spx_pnl             =  diff(spx_close)
    spx_ret             =  array([ log(spx_close[i] / spx_close[i - 1]) for i in range(1, len(spx_close)) ])
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

    # trader statistics

    pnl                 =  [ sum(pnls[date]) for date in dates ]
    balance             =  cumsum([ init_balance ] + pnl)
    returns             =  array([ log(balance[i] / balance[i - 1]) for i in range(1, len(balance)) ])
    cum_ret             =  cumsum(returns)
    mu                  =  mean(returns)
    sigma               =  std(returns)
    sharpe              =  mu / sigma * sqrt(252)
    mean_p_val          =  mean_bootstrap(returns)
    sharpe_res          =  sharpe_bootstrap(returns)
    sharpe_ci_lo        =  sharpe_res[0]
    sharpe_ci_hi        =  sharpe_res[1]
    sharpe_p_val        =  sharpe_res[2]
    drawdowns           =  [ cum_ret[i] - max(cum_ret[0:i + 1]) for i in range(len(cum_ret)) ]
    h_drawdowns         =  mc_drawdown(returns)
    p_95_h_dd           =  h_drawdowns[int(N * 0.95)]
    max_dd              =  min(drawdowns)
    mean_dd             =  mean(drawdowns)
    mar_ratio           =  (mu * 252) / abs(max_dd)
    gross_profit        =  sum([ i for i in pnl if i > 0 ])
    gross_loss          =  sum([ abs(i) for i in pnl if i < 0 ])
    profit_factor       =  gross_profit / gross_loss

    # OLS alpha 

    model   =  LinearRegression()
    
    model.fit(spx_ret.reshape(-1, 1), returns)

    X           = arange(min(spx_ret), max(spx_ret), step = 0.00001)
    Y           = model.predict(X.reshape(-1, 1))
    b           = model.coef_[0]
    a           = model.intercept_
    corr        = corrcoef(spx_ret, returns)[0, 1]

    pass

    if DEBUG == 5:

        print(f"{'alpha':20}{a:0.4f}")
        print(f"{'beta':20}{b:0.4f}")

        print(f"dates[0] = {dates[0]}")
        print(f"ln({spx_close[1]:0.2f}, {spx_close[0]:0.2f}) = {log(spx_close[1] / spx_close[0]):0.4f}, ref = {spx_ret[0]:0.4f}")
        print(f"ln({balance[1]:0.2f} / {balance[0]:0.2f})    = {log(balance[1] / balance[0]):0.4f}, ref = {returns[0]:0.4f}")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                {
                    "x":    spx_ret,
                    "y":    returns,
                    "mode": "markers",
                    "name": "returns",
                    "text": dates
                }
            )
        )

        fig.add_trace(
            go.Scatter(
                {
                    "x":        X,
                    "y":        Y,
                    "line":     { "color": "#FF00FF" },
                    "name":     "model"
                }
            )
        )

        fig.show()

    if DEBUG == 2:

        print(f"{'TOTAL':20}{sum(pnl):>15.2f}\n")

    print(f"\ntest period")
    print("\n-----\n")
    print(f"{'date range:':20}{start:>15}{end:>15}")
    print(f"{'trading days:':20}{len(dates):>15}")
    print("\ntotals")
    print("\n-----\n")
    print(f"{'initial balance':20}{init_balance:>15.2f}")
    print(f"{'ending balance':20}{balance[-1]:>15.2f}")
    print(f"{'pnl':20}{balance[-1] - init_balance:>15.2f}")
    print(f"{'return':20}{(balance[-1] / init_balance - 1) * 100:>15.2f}%")
    print("\n-----\n")
    print(f"{'summary statistics':20}{'trader':>15}{'spx':>15}\n")
    print(f"{'daily return:':20}{mu * 100:>15.2f}%{spx_mu * 100:>15.2f}%")
    print(f"{'daily stdev:':20}{sigma * 100:>15.2f}%{spx_sigma * 100:>15.2f}%")
    print(f"{'annualized return:':20}{mu * 252 * 100:>15.2f}%{spx_mu * 252 * 100:>15.2f}%")
    print(f"{'annualized stdev:':20}{sigma * sqrt(252) * 100:>15.2f}%{spx_sigma * sqrt(252) * 100:>15.2f}%")
    print(f"{'max drawdown:':20}{max_dd * 100:>15.2f}%{spx_max_dd * 100:>15.2f}%")
    print(f"{'avg. drawdown:':20}{mean_dd * 100:>15.2f}%{spx_mean_dd * 100:>15.2f}%")
    print(f"{'mc drawdown, p95:':20}{-p_95_h_dd * 100:>15.2f}%{'-':>15}")
    print(f"{'MAR ratio:':20} {mar_ratio:>15.2f}{spx_mar_ratio:>15.2f}")
    print(f"{'profit factor:':20} {profit_factor:>15.2f}{spx_profit_factor:>15.2f}")
    print(f"{'alpha:':20} {a:>15.4f}{'-':>15}")
    print(f"{'beta:':20} {b:>15.4f}{'-':>15}")
    print(f"{'correlation:':20} {corr:>15.4f}{'-':>15}")
    print(f"{'sharpe ratio:':20} {sharpe:>15.2f}{spx_sharpe:>15.2f}")
    print(f"{'p(mean > 0):':20} {mean_p_val:>15.2f}{'-':>15}")
    print(f"{'sharpe 95% ci lo:':20} {sharpe_ci_lo:>15.2f}{'-':>15}")
    print(f"{'sharpe 95% ci hi:':20} {sharpe_ci_hi:>15.2f}{'-':>15}")
    print(f"{'p(sharpe > index):':20} {sharpe_p_val:>15.2f}{'-':>15}")
    print("\n")

    if DEBUG == 6:

        fig = go.Figure()

        adjusted_returns        = returns * (spx_sigma / sigma)
        adjusted_returns        = adjusted_returns - (mean(adjusted_returns) - spx_mu)
        adjusted_cum_returns    = cumsum(adjusted_returns)

        traces = [
            ( "trader", cum_ret, "#0000FF", "y1" ),
            ( "spx", spx_cum_ret, "#FF0000", "y1" ),
            ( "scaled", adjusted_cum_returns, "#FF00FF", "y1" )
        ]

        for trace in traces:
        
            fig.add_trace(
                go.Scatter(
                    {
                        "x":        dates,
                        "y":        trace[1],
                        "name":     f"{trace[0]} returns",
                        "marker":   { "color": trace[2] },
                        "yaxis":    trace[3]
                    }
                )
            )

        fig.update_layout(
            yaxis2 = { "overlaying": "y", "side": "right" }
        )
        fig.show()

    print(f"{time() - t0:0.1f}s\n")

    pass