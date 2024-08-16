from    bisect                  import  bisect_left
from    config                  import  FUT_DEFS
from    datetime                import  datetime
from    os.path                 import  join
from    math                    import  log, sqrt
from    numpy                   import  array, cumsum, mean, nonzero, std
from    parsers                 import  ninjatrader, tradovate, thinkorswim
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_sc_df, in_row
from    typing                  import  List


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
    tz:     str,
    debug:  bool
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

    if debug == 1:
        
        print(symbol, "\n")
        print(df.tail())
        print(res)

    if debug == 2:

        print(f"{symbol:20}{sum(pnl):>10.2f}")

    res = res.group_by("date", maintain_order = True).agg(
                    [ 
                        pl.col("pnl").sum().alias("pnl"),
                    ]
                )

    return res


def bootstrap(returns: List[float]):

    pass


if __name__ == "__main__":

    t0              = time()
    in_fn           = join(".", "csvs", f"{argv[1]}.csv")
    tz              = argv[2]
    parser          = PARSERS[argv[3]]
    init_balance    = float(argv[4])
    debug           = int(argv[5])
    in_rows         = parser.parse(in_fn, tz, None, 0)
    symbols         = sorted(set([ row[in_row.symbol] for row in in_rows ]))
    pnls            = {}

    for symbol in symbols:

        sym_rows = [ row for row in in_rows if row[in_row.symbol] == symbol ]
        res      = get_daily(symbol, sym_rows, tz, debug)

        if debug == 3:
            
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

    mask        = nonzero(mask)
    dates       = dates[mask]
    pnl         =  [ sum(pnls[date]) for date in dates ]
    cum_pnl     =  cumsum(pnl)
    cum_pnl     += init_balance
    returns     =  [ log(cum_pnl[i] / cum_pnl[i - 1]) for i in range(1, len(cum_pnl)) ]
    cum_ret     =  cumsum(returns)
    dates       =  dates[1:]

    if debug == 2:

        print(f"{'TOTAL':20}{sum(pnl):>10.2f}\n")


    # results

    mu      = mean(returns)
    sigma   = std(returns)
    sharpe  = mu / sigma * sqrt(252)

    print("\ntotals")
    print("\n-----\n")
    print(f"{'initial balance':20}{init_balance:>10.2f}")
    print(f"{'ending balance':20}{cum_pnl[-1]:>10.2f}")
    print(f"{'pnl':20}{cum_pnl[-1] - init_balance:>10.2f}")
    print(f"{'return':20}{(cum_pnl[-1] / init_balance - 1) * 100:>10.2f}%")
    print("\n-----\n")
    print("summary statistics (trader)\n")
    print(f"{'daily return:':20}{mu * 100:>10.2f}%")
    print(f"{'daily stdev:':20}{sigma * 100:>10.2f}%")
    print(f"{'annualized return:':20}{mu * 252 * 100:>10.2f}%")
    print(f"{'annualized stdev:':20}{sigma * sqrt(252) * 100:>10.2f}%")
    print(f"{'sharpe:':20}{sharpe:>10.2f}")
    print("\n")

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            {
                "x":    dates,
                "y":    cum_ret,
                "name": "returns"
            }
        )
    )

    fig.show()

    print(f"{time() - t0:0.1f}s")

    pass