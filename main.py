from    bisect                  import  bisect_left
from    os.path                 import  join
from    math                    import  sqrt
from    numpy                   import  cumsum, diff, log, mean, nonzero, std, zeros
from    numpy.random            import  choice
from    polars                  import  col, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    time                    import  time
from    util                    import  get_sym_data


# python get_history.py 20240709_out ohlcv-1s Europe/Berlin


DBN_DT_FMT          = "%Y-%m-%dT%H:%M:%S"
PERIODS_PER_YEAR    = {
                        "ohlcv-1s": 23 * 60 * 60 * 252
                    }
N                   = 10_000


if __name__ == "__main__":

    t0          = time()
    fn          = join(".", "csvs", f"{argv[1]}.csv")
    hist        = read_csv(fn)
    symbols     = list(hist["symbol"].unique())
    schema      = argv[2]
    tz          = argv[3]
    ppy         = PERIODS_PER_YEAR[schema]
    sym_data    = get_sym_data(symbols, schema, DBN_DT_FMT, tz)

    for symbol in symbols:

        rows        = hist.filter(col("symbol") == symbol).rows()
        start       = rows[0][-2]
        stop        = rows[-1][-2]
        prices      = sym_data[symbol]["open"]
        chgs        = diff(prices)
        logs        = diff(log(prices))
        position    = zeros(len(chgs))

        for row in rows:

            position[row[-2]:]+= row[-1]
        
        mask    = nonzero(position)
        pnl     = cumsum(chgs * position)[mask]
        X       = [ i for i in range(len(pnl)) ]
        ret     = logs * position
        retc    = cumsum(ret)[-1]
        mu      = mean(ret)
        sigma   = std(ret)
        sharpe  = mu / sigma * sqrt(ppy)

        print(f"\n{symbol}\n")

        print(f"{'pnl (pt):':20}{pnl[-1]:>10.0f}")
        print(f"{'return:':20}{retc * 100:>10.2f}%")

        print("\nannualized\n")
        
        print(f"{'return:':20}{mu * ppy * 100:>10.2f}%")
        print(f"{'stdev:':20}{sigma * sqrt(ppy) * 100:>10.2f}%")
        print(f"{'sharpe:':20}{sharpe:>10.2f}")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                {
                    "x":        X,
                    "y":        pnl,
                    "name":     "pnl"
                }
            )
        )

        fig.show()

        # monte carlo permutation

        returns = []

        # detrend

        mu_sym      = mean(logs)
        logs        = logs - mu_sym

        for _ in range(N):

            logs    = choice(logs, size = len(logs), replace = True)
            mu_i    = mu(position * logs)
            
            returns.append(mu_i)

        fig = go.Figure()

        fig.add_trace(go.Histogram(x = returns, name = "sampling distribution (mean return)"))
        fig.add_vline(x = retc, line_color = "#FF00FF")

        returns = sorted(returns)
        i       = bisect_left(returns, mu)
        p_val   = 1 - i / len(returns)

        fig.show()

        print(f"{'num samples':20}{N:>10}")
        print(f"{'sampling mean:':20}{mean(returns) * 100:>10.2f}%")
        print(f"{'sampling stdev:':20}{std(returns) * 100:>10.2f}%")
        print(f"{f'p(r = {mu * 100:>0.2f}%)':20}:{p_val:>10.2f}")
        print("\n")

        print(f"{time() - t0:0.1f}s")