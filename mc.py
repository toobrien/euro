
from    bisect                  import  bisect_left
from    datetime                import  datetime, timedelta
from    os.path                 import  join
from    math                    import  sqrt
from    numpy                   import  add, array, cumsum, diff, log, mean, nonzero, std, zeros
from    numpy.random            import  default_rng
from    polars                  import  DataFrame, col, Config, read_csv
import  plotly.graph_objects    as      go
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_benchmark, get_sym_data, out_row, sharpe_htest


# python mc.py euro_out sc Europe/Berlin NQ 0


Config.set_tbl_rows(-1)


RFR                 = log(1 + 0.052) / 252
MAX_PNL_CHART_LEN   = 100_000
PERIODS_PER_YEAR    = 23 * 60 * 60 * 252
N                   = 10_000


if __name__ == "__main__":

    t0          = time()
    fn          = join(".", "csvs", f"{argv[1]}.csv")
    src         = argv[2]
    tz          = argv[3]
    symbol      = argv[4]
    debug       = int(argv[5])
    out         = read_csv(fn).filter(col("symbol") == symbol)
    rows        = out.rows()
    start       = out["ts"][0].split("T")[0]
    end         = (datetime.strptime(out["ts"][-1].split("T")[0], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    sym_data    = get_sym_data([ symbol ], start, end, tz, src)
    prices      = sym_data[symbol]["close"]

    # adjust for error

    for row in rows:

        prices[row[out_row.idx]] = row[out_row.in_price]

    chgs        = diff(prices)
    logs        = diff(log(prices))
    position    = zeros(len(chgs))

    for row in rows:

        position[row[out_row.idx]:] += row[out_row.pos_chg]

    if debug == 1:

        # check for invalid prices

        ts      = sym_data[symbol]["ts"][1:]
        prices  = prices[1:]

        for i in range(len(prices)):

            if prices[i] <= 0:

                print(ts[i], i, prices[i])

        exit()

    # correct floating point error

    position    = [ i if abs(i) > 1e-10 else 0 for i in position ]
    position_r  = [ pos / abs(pos) if pos != 0 else 0 for pos in position ]

    if debug == 2:

        # set i and j to sample trade start idxs from outfile

        i       = 24328
        j       = 24360
        n       = j - i
        pnls    = chgs * position
        df      = DataFrame(
                {
                    "ts":       sym_data[symbol]["ts"][i:j],
                    "pos":      position[i:j],
                    "prices":   prices[i:j],
                    "chgs":     chgs[i:j],
                    "chgs_cum": cumsum(chgs[i:j]),
                    "diff_0":   [ prices[x] - prices[i] for x in range(i, j) ],
                    "pnl_cum":  cumsum(pnls[i:j])
                }
            )

        print(df)

        exit()
    
    ret             = logs * position_r
    pnl             = chgs * position
    benchmark       = get_benchmark(symbol, start, end, tz)
    bench_ret       = diff(benchmark["close"].log())
    bench_date      = [ date.split("T")[0] for date in benchmark["datetime"] ][1:]
    idx             = [ bisect_left(sym_data[symbol]["ts"], date) for date in bench_date ]
    trader_pnl      = add.reduceat(pnl, idx)
    trader_ret      = add.reduceat(ret, idx)
    day_df          = DataFrame(
                        {
                            "date":                         bench_date,
                            "trader pnl":                   trader_pnl,
                            "trader pnl (cum)":             cumsum(trader_pnl),
                            "trader return (%)":            trader_ret * 100,
                            "trader return (cum %)":        cumsum(trader_ret) * 100,
                            f"{symbol} return (%)":         bench_ret * 100,
                            f"{symbol} returns (cum %)":    cumsum(bench_ret) * 100
                        }
                    )

    # remaining statistics

    trader_mu       = trader_ret.mean()
    trader_sig      = trader_ret.std()
    trader_sharpe   = trader_mu / trader_sig * sqrt(252)
    bench_mu        = mean(bench_ret)
    bench_sig       = std(bench_ret)
    bench_sharpe    = bench_mu / bench_sig * sqrt(252)
    sharpe_res      = sharpe_htest(bench_ret, trader_ret, RFR, 0.05)
    p_eq_0          = sharpe_res["p_eq_0"]
    p_inferior      = sharpe_res["p_b_lt_a"]
    sr_diff         = sharpe_res["sr_diff"]

    print()
    print(symbol)
    print("-----\n")

    print(day_df)

    print("\n(all returns are unweighted)\n")

    print(f"{'':20}{'trader':>20}{'benchmark':>20}\n")
    print(f"{'daily return':20}{trader_mu * 100:>19.2f}%{bench_mu * 100:>19.2f}%")
    print(f"{'daily stdev':20}{trader_sig * 100:>19.2f}%{bench_sig * 100:>19.2f}%")
    print(f"{'ann. return':20}{trader_mu * 252 * 100:>19.2f}%{bench_mu * 252 * 100:>19.2f}%")
    print(f"{'ann. stdev':20}{trader_sig * sqrt(252) * 100:>19.2f}%{bench_sig * sqrt(252) * 100:>19.2f}%")
    print(f"{'sharpe':20}{trader_sharpe:>20.2f}{bench_sharpe:>19.2f}")

    # pnl chart

    unit    = "1s"
    mask    = nonzero(position)
    Y       = cumsum(chgs * position)[mask]
    X       = [ i for i in range(len(Y)) ]

    if debug == 2:

        with open("./debug.txt", "w") as fd:
        
            T   = sym_data[symbol]["ts"][mask]
            POS = position[mask]
            Y_  = prices[mask]

            if len(Y) > MAX_PNL_CHART_LEN:

                T   = [ T[i] for i in range(60, len(T), 60) ]    
                Y   = [ Y[i] for i in range(60, len(Y), 60) ]
                POS = [ POS[i] for i in range(60, len(POS), 60) ]
                Y_  = [ Y_[i] for i in range(60, len(Y_), 60) ]

            for i in range(len(T)):

                fd.write(",".join([ T[i], f"{POS[i]:0.2f}", f"{Y[i]:0.2f}", f"{Y_[i]:0.2f}" ]) + "\n")

        print("debug finished")

        exit()

    if len(Y) > MAX_PNL_CHART_LEN:

        # too much data, reduce to 1-m chart

        unit    = "1m"
        Y       = [ Y[i] for i in range(60, len(Y), 60) ]
        X       = [ i for i in range(len(Y)) ]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            {
                "x":    X,
                "y":    Y,
                "name": "pnl"
            }
        )
    )

    fig.update_layout(title = f"{symbol} {unit} pnl")
    fig.show()

    # benchmark chart

    fig = go.Figure()

    adjusted_ret = bench_ret * trader_sig / bench_sig

    traces = [
        ( cumsum(trader_ret), "trader", "#0000FF" ),
        ( cumsum(trader_mu), f"{symbol}", "#FF0000" ),
        ( cumsum(adjusted_ret), f"{symbol} (adjusted)", "#FF00FF" )
    ]

    for trace in traces:

        fig.add_trace(
            go.Scatter(
                {
                    "x":        bench_date,
                    "y":        trace[0],
                    "name":     trace[1],
                    "marker":   { "color": trace[2] }
                }
            )
        )
    
    fig.update_layout(title = f"{symbol} returns (daily, unweighted)")
    fig.show()

    # monte carlo permutation

    sampling_dist   = []

    # detrend
    
    sym_mu          = mean(logs)
    logs            = logs - sym_mu
    rng             = default_rng()

    t1 = time()

    for i in range(N):

        rng.shuffle(logs)

        r_      = add.reduceat(position_r * logs, idx)
        mu_i    = mean(r_)
        
        sampling_dist.append(mu_i)

        if i % 100 == 0:
            
            print(f"iter {i}/{N}, {time() - t1:0.1f}s\r")

    fig = go.Figure()

    fig.add_trace(go.Histogram(x = sampling_dist, name = "sampling distribution (mean return)"))
    fig.add_vline(x = trader_mu, line_color = "#FF00FF")

    sampling_dist   = sorted(sampling_dist)
    i               = bisect_left(sampling_dist, trader_mu)
    p_val           = 1 - i / len(sampling_dist)

    fig.show()

    print("\nmonte carlo\n")

    print(f"{'num samples':20}{N:>20}")
    print(f"{'return period:':20}{'1 second':>20}")
    print(f"{'trader mean:':20}{trader_mu * 100:>19.9f}%")
    print(f"{'sampling mean:':20}{mean(sampling_dist) * 100:>19.9f}%")
    print(f"{'sampling stdev:':20}{std(sampling_dist) * 100:>19.9f}%")
    print(f"{f'p(not predictive):':20}{p_val:>19.2f}")
    
    print("\nsharpe test\n")

    print(f"{'p(sharpe == index):':20} {p_eq_0:>19.2f}")
    print(f"{'p(sharpe <= index):':20} {p_inferior:>19.2f}")
    
    print("\n")

    print(f"{time() - t0:0.1f}s")