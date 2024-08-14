from    os.path import  join
from    parsers import  ninjatrader, tradovate, thinkorswim
import  polars  as      pl
from    sys     import  argv
from    time    import  time
from    util    import  get_sc_df, in_row
from    typing  import  List

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

        df = get_sc_df(symbol, tz, True).drop(["date", "time"])

        print(symbol, "\n")
        print(df.tail())

        pass
    
    except FileNotFoundError:

        print(f"{symbol} daily bars not found")

    pass

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