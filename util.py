from    datetime    import  datetime
from    config      import  DBN_PATH, FUT_DEFS, SC_PATH, SC_TZ, TS_FMT
from    numpy       import  array
from    os.path     import  join
import  polars      as      pl
from    pytz        import  timezone
from    typing      import  List


def get_dbn_df(
    symbol: str,
    out_tz: int
) -> pl.DataFrame:
    
    df  = pl.read_csv(
            join(DBN_PATH, f"{symbol}.c.0_ohlcv-1s.csv")
        ).with_columns(
            pl.col(
                "ts_event"
            ).cast(
                pl.Datetime
            ).dt.convert_time_zone(
                out_tz
            ).dt.strftime(
                TS_FMT
            ).alias(
                "ts"
            )
        )

    return df


def get_sc_df(
    symbol: str,
    out_tz: str
) -> pl.DataFrame:

    sc_offset   = datetime.now(timezone(SC_TZ)).strftime("%z")
    df          = pl.read_csv(
                    join(SC_PATH, f"{symbol}.scid_BarData.txt"),
                    new_columns         = [ "date", "time", "open", "high", "low", "close" ],
                    schema_overrides    = {
                                            "Date":     str,
                                            " Time":    str,
                                            " Open":    pl.Float32,
                                            " High":    pl.Float32,
                                            " Low":     pl.Float32,
                                            " Last":    pl.Float32,
                                        }
                ).with_columns(
                    (
                        pl.col("date") + pl.col("time")
                    ).str.strptime(
                        pl.Datetime, "%Y/%m/%d %H:%M:%S"
                    ).dt.replace_time_zone(
                        SC_TZ
                    ).dt.convert_time_zone(
                        out_tz
                    ).dt.strftime(
                        TS_FMT
                    ).alias(
                        "ts"
                    )
                )
    
    return df


def get_ohlcv(
    symbol:     str,
    start:      str,
    end:        str,
    out_tz:     str,
    src:        str
):

    if src == "dbn":

        df  = get_dbn_df(symbol, out_tz)
    
    elif src == "sc":
    
        df = get_sc_df(symbol, out_tz)

    df      = df.filter((pl.col("ts") >= start) & (pl.col("ts") <= end))
    ts      = array(df["ts"])
    open    = array(df["open"])
    high    = array(df["high"])
    low     = array(df["low"])
    close   = array(df["close"])

    return ts, open, high, low, close


def get_sym_data(
    symbols:    List[str],
    start:      str,
    end:        str,
    tz:         str,
    src:        str,
):

    sym_data = {}

    for symbol in symbols:

        if symbol not in FUT_DEFS or not FUT_DEFS[symbol]["enabled"]:

            continue

        elif FUT_DEFS[symbol]["alias"]:

            symbol = FUT_DEFS[symbol]["alias"]

        ts, open, high, low, close = get_ohlcv(symbol, start, end, tz, src)

        sym_data[symbol] = {
                            "ts":       ts,
                            "open":     open,
                            "high":     high,
                            "low":      low,
                            "close":    close
                        }

    return sym_data