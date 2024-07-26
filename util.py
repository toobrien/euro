from    config  import  FUT_DEFS
from    numpy   import  array
import  polars  as      pl
from    typing  import  List


def adjust_tz(
    df:         pl.DataFrame,
    from_col:   str,
    to_col:     str, 
    FMT:        str, 
    tz:         int
) -> pl.DataFrame:
    
    df = df.with_columns(
        pl.col(
            from_col
        ).cast(
            pl.Datetime
        ).dt.convert_time_zone(
            tz
        ).dt.strftime(
            FMT
        ).alias(
            to_col
        )
    )

    return df


def get_sym_data(
    symbols:    List[str],
    schema:     str,
    fmt:        str, 
    tz:         str
):

    sym_data = {}

    for symbol in symbols:

        if symbol not in FUT_DEFS or not FUT_DEFS[symbol]["enabled"]:

            continue

        ohlcv       = pl.read_csv(f"../databento/csvs/{symbol}.c.0_{schema}.csv")
        ohlcv       = adjust_tz(ohlcv, "ts_event", "ts_event", fmt, tz)
        ts          = [ t.split(".")[0] for t in array(ohlcv["ts_event"]) ]

        sym_data[symbol] = {
            "ts":       ts,
            "open":     array(ohlcv["open"]),
            "high":     array(ohlcv["high"]),
            "low":      array(ohlcv["low"]),
            "close":    array(ohlcv["close"])
        }
    
    return sym_data