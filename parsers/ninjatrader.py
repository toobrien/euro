from    config      import  FUT_DEFS
import  polars      as      pl
from    sys         import  path

path.append(".")

from    config      import  TS_FMT

pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(-1)


NT_DT_FMT = "%m/%d/%Y %I:%M:%S %p"


def parse(in_fn: str):
    
    input   = []
    trades  = pl.read_csv(in_fn)
    trades  = trades.with_columns(
                [
                    pl.col("Entry time").str.strptime(pl.Datetime, NT_DT_FMT).dt.strftime(TS_FMT).alias("Entry time"),
                    pl.col("Exit time").str.strptime(pl.Datetime, NT_DT_FMT).dt.strftime(TS_FMT).alias("Exit time")
                ]
            )

    for i in range(trades.height):

        instrument  = trades["Instrument"][i].split()[0]
        scale       = 1.0
        
        if FUT_DEFS[instrument]["alias"]:

            scale       = FUT_DEFS[instrument]["scale"]
            instrument  = FUT_DEFS[instrument]["alias"]

        market_pos  = 1 if trades["Market pos."][i] == "Long" else -1
        qty         = trades["Qty"][i] * market_pos * scale
        entry_time  = trades["Entry time"][i]
        entry_price = trades["Entry price"][i]
        exit_time   = trades["Exit time"][i]
        exit_price  = trades["Exit price"][i]

        input.append(( instrument, entry_time, qty, entry_price ))
        input.append(( instrument, exit_time, qty * -1, exit_price ))

    input = sorted([ row for row in input ], key = lambda r: r[1])

    return input