from    os.path import  join
from    parsers import  ninjatrader, tradovate, thinkorswim
import  polars  as      pl
from    sys     import  argv
from    time    import  time


PARSERS = { 
            "tradovate":    tradovate,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


# python wrc.py tydal_in America/New_York thinkorswim


if __name__ == "__main__":

    t0      = time()
    in_fn   = join(".", "csvs", f"{argv[1]}.csv")
    tz      = argv[2]
    parser  = PARSERS[argv[3]]
    in_rows = parser.parse(in_fn, tz, None, 0)

    print(f"{time() - t0:0.1f}s")

    pass