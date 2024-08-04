from    datetime    import  datetime, timedelta
import  polars      as      pl
from    sys         import  path

path.append(".")

from    config      import  TS_FMT
from    util        import get_sym_data

pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(-1)


NT_DT_FMT = "%m/%d/%Y %I:%M:%S %p"


def parse(
    in_fn:      str,
    tz:         str,
    src:        str
):
    
    trades      = pl.read_csv(in_fn)
    trades      = trades.with_columns(
                    [
                        pl.col("Entry time").str.strptime(pl.Datetime, NT_DT_FMT).dt.strftime(TS_FMT).alias("Entry time"),
                        pl.col("Exit time").str.strptime(pl.Datetime, NT_DT_FMT).dt.strftime(TS_FMT).alias("Exit time")
                    ]
                )
    input       = []
    symbols     = set([ sym.split()[0] for sym in trades["Instrument"] ])
    start       = trades["Entry time"][0].split("T")[0]
    end         = trades["Exit time"][-1].split("T")[0]
    end         = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    sym_data    = get_sym_data(symbols, start, end, tz, src)

    print(trades.head())

    exit()

    return None, None