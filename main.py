from    datetime                import  datetime
from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    os.path                 import  join
from    math                    import  sqrt
from    numpy                   import  array, cumsum, diff, log, nonzero, zeros
from    polars                  import  col, Config, Datetime, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    util                    import  adjust_tz, get_sym_data


# python get_history.py 20240709_out ohlcv-1s Europe/Berlin


DBN_DT_FMT          = "%Y-%m-%dT%H:%M:%S"
PERIODS_PER_YEAR    = {
                        "ohlcv-1s": 23 * 60 * 60 * 252
                    }


if __name__ == "__main__":

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

        print(logs[0])

        for row in rows:

            position[row[-2]:]+= row[-1]
        
        mask    = nonzero(position)
        pnl     = cumsum(chgs * position)[mask]
        ret     = cumsum(logs * position)[mask]
        X       = [ i for i in range(len(pnl)) ]

        print(f"{symbol} pnl (pt):  {pnl[-1]}")
        print(f"{symbol} ret (pt):  {ret[-1]:0.4f}")

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

    pass