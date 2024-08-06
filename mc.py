
from    bisect                  import  bisect_left
from    datetime                import  datetime, timedelta
from    os.path                 import  join
from    math                    import  sqrt
from    numpy                   import  cumsum, diff, log, mean, nonzero, std, zeros
from    numpy.random            import  default_rng
from    polars                  import  DataFrame, col, Config, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_sym_data


# python mc.py euro_out sc Europe/Berlin


Config.set_tbl_rows(-1)


MAX_PNL_CHART_LEN   = 100_000
PERIODS_PER_YEAR    = 23 * 60 * 60 * 252
N                   = 10_000


if __name__ == "__main__":

    t0          = time()
    fn          = join(".", "csvs", f"{argv[1]}.csv")
    hist        = read_csv(fn)
    symbols     = list(hist["symbol"].unique())
    src         = argv[2]
    tz          = argv[3]
    debug       = int(argv[4])
    start       = hist["ts"][0].split("T")[0]
    end         = (datetime.strptime(hist["ts"][-1].split("T")[0], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    sym_data    = get_sym_data(symbols, start, end, tz, src)

    for symbol in symbols:

        rows        = hist.filter(col("symbol") == symbol).rows()
        start       = rows[0][-3]
        stop        = rows[-1][-3]
        prices      = sym_data[symbol]["close"]
        chgs        = diff(prices)
        logs        = diff(log(prices))
        position    = zeros(len(chgs))

        for row in rows:

            position[row[-3]:]+= row[-2]

        if debug:

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
        
        ret         = logs * position
        pnls        = chgs * position
        t_pnl_sec   = sum(pnls)
        sec_total   = cumsum(ret)[-1]
        mu_sec      = mean(ret)
        sigma_sec   = std(ret)
        sharpe_sec  = mu_sec / sigma_sec * sqrt(PERIODS_PER_YEAR)
        date        = [ t.split("T")[0] for t in sym_data[symbol]["ts"] ]
        day_df      = DataFrame({ "date": date[1:], "pnl": pnls, "return (%)": ret })
        day_df      = day_df.group_by("date", maintain_order = True).agg(
                        [ 
                            col("pnl").sum().alias("pnl"),
                            (col("return (%)").sum() * 100).alias("return (%)"),
                        ]
                    )
        day_df      = day_df.with_columns(col("pnl").cum_sum().alias("pnl_cum"))
        day_df      = day_df.with_columns(col("return (%)").cum_sum().alias("return_cum (%)"))
        mu_day      = day_df["return (%)"].mean()
        sigma_day   = day_df["return (%)"].std()
        sharpe_day  = mu_day / sigma_day * 16

        print()
        print(symbol)
        print("-----\n")

        print(day_df)

        print(f"\n{'':20}{'annualized':>20}{'daily':>20}{'1-second':>20}\n")
        print(f"{'return':20}{mu_day * 256:>19.2f}%{mu_day:>19.4f}%{mu_sec * 100:>19.9f}%")
        print(f"{'stdev':20}{sigma_day * 16:>19.2f}%{sigma_day:>19.4f}%{sigma_sec * 100:>19.9f}%")
        print(f"{'sharpe':20}{sharpe_day:>20.2f}")

        # pnl chart

        mask    = nonzero(position)
        Y       = cumsum(chgs * position)[mask]
        X       = [ i for i in range(len(Y)) ]

        if len(Y) > MAX_PNL_CHART_LEN:

            # too much data, reduce to 1-mi chart

            Y = [ Y[i] for i in range(60, len(Y), 60) ]
            X = [ i for i in range(len(Y)) ]

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

        fig.show()

        # monte carlo permutation

        sampling_dist   = []
        mu_sym          = mean(logs)
        logs            = logs - mu_sym     # detrend
        rng             = default_rng()

        for _ in range(N):

            rng.shuffle(logs)

            mu_i = mean(position * logs)
            
            sampling_dist.append(mu_i)

        fig = go.Figure()

        fig.add_trace(go.Histogram(x = sampling_dist, name = "sampling distribution (mean return)"))
        fig.add_vline(x = mu_sec, line_color = "#FF00FF")

        sampling_dist   = sorted(sampling_dist)
        i               = bisect_left(sampling_dist, mu_sec)
        p_val           = 1 - i / len(sampling_dist)

        fig.show()

        print("\nmonte carlo\n")

        print(f"{'num samples':20}{N:>20}")
        print(f"{'return period:':20}{'1 second':>20}")
        print(f"{'trader mean:':20}{mu_sec * 100:>19.9f}%")
        print(f"{'sampling mean:':20}{mean(sampling_dist) * 100:>19.9f}%")
        print(f"{'sampling stdev:':20}{std(sampling_dist) * 100:>19.9f}%")
        print(f"{f'p(r >= {mu_sec * 100:>0.9f}%):':20}{p_val:>19.2f}")
        print("\n")

        print(f"{time() - t0:0.1f}s")