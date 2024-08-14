from    bisect  import  bisect_left
from    os.path import  join
from    numpy   import  array
from    parsers import  ninjatrader, tradovate, thinkorswim
import  polars  as      pl
from    sys     import  argv
from    time    import  time
from    util    import  get_sc_df, in_row
from    typing  import  List


DEBUG   = False
PARSERS = { 
            "tradovate":    tradovate,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)


# python wrc.py tydal_in America/New_York thinkorswim


def get_daily(symbol: str, rows: List, tz: str):

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

        trade_ts    = [ row[in_row.ts] for row in rows ]
        trade_px    = [ row[in_row.price] for row in rows ]
        trade_qty   = [ row[in_row.qty] for row in rows ]
        trades      = list(zip(trade_ts, trade_px, trade_qty))
        settles     = list(zip(df["ts"], df["close"], [ 0 for i in range(df.height) ]))
        
        start       = bisect_left(settles, rows[0][in_row.ts], key = lambda r: r[0]) - 1
        end         = bisect_left(settles, rows[-1][in_row.ts], key = lambda r: r[0]) + 2
        settles     = settles[start:end]
        combined    = sorted(trades + settles, key = lambda r: r[0])
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

        if combined[0][2] != 0:

            print(f"{symbol}: warning, trade prior to data start")

        if combined[-1][2] != 0:

            print(f"{symbol}: warning, unclosed position at data end")

        if DEBUG:

            print(symbol, "\n")
            print(df.tail())

            dbg_df = pl.DataFrame(
                        {
                            "ts":       [ row[0] for row in combined ],
                            "price":    [ row[1] for row in combined ],
                            "qty":      [ row[2] for row in combined ],
                            "pos":      position,
                            "pnl":      pnl
                        }
                    )
            
            print(dbg_df)

        pass

    except FileNotFoundError:

        print(f"{symbol} daily bars not found")

    return {}


if __name__ == "__main__":

    t0      = time()
    in_fn   = join(".", "csvs", f"{argv[1]}.csv")
    tz      = argv[2]
    parser  = PARSERS[argv[3]]
    in_rows = parser.parse(in_fn, tz, None, 0)
    symbols = set([ row[in_row.symbol] for row in in_rows ])

    for symbol in symbols:

        sym_rows = [ row for row in in_rows if row[in_row.symbol] == symbol ]
        
        get_daily(symbol, sym_rows, tz)

        pass

    print(f"{time() - t0:0.1f}s")

    pass