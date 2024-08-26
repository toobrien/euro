from    bisect      import  bisect_left
from    datetime    import  datetime
from    config      import  DBN_PATH, FUT_DEFS, SC_PATH, SC_TZ, SPX_PATH, TS_FMT
from    enum        import  IntEnum
from    numpy       import  array
from    os.path     import  join
import  polars      as      pl
from    pytz        import  timezone
from    typing      import  List


class in_row(IntEnum):

    symbol  = 0
    ts      = 1
    qty     = 2
    price   = 3


class out_row(IntEnum):

    symbol      = 0
    ts          = 1
    idx         = 2
    pos_chg     = 3
    in_price    = 4
    out_price   = 5


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
    out_tz: str,
    daily:  bool = False
) -> pl.DataFrame:
    

    suffix = ".scid_BarData.txt" if not daily else ".dly_BarData.txt"

    df  = pl.read_csv(
            join(SC_PATH, f"{symbol}{suffix}"),
            new_columns         = [ "date", "time", "open", "high", "low", "close" ],
            schema_overrides    = {
                                    "Date":     str,
                                    " Time":    str,
                                    " Open":    pl.Float32,
                                    " High":    pl.Float32,
                                    " Low":     pl.Float32,
                                    " Last":    pl.Float32,
                                }
        )
    
    if daily:

        # replace null (midnight) times in SC with SETTLEMENT_TIME
        # probably doesn't work with daylight savings, but not a big deal

        settle_tz       = timezone("America/New_York")
        local_tz        = timezone(SC_TZ)
        settlement_dt   = datetime.strptime("16:00:00", "%H:%M:%S").replace(tzinfo = settle_tz)
        settlement_dt   = settlement_dt.astimezone(local_tz)

        # minute conversion seems broken for some reason, so hard code %M and %S

        settlement_ts   = settlement_dt.strftime("%H:00:00")
        df              = df.with_columns(pl.lit(f" {settlement_ts}").alias("time"))

    df = df.with_columns(
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

        if symbol in FUT_DEFS and FUT_DEFS[symbol]["alias"]:

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


def get_spx(start_plus_one, end):

    df      = pl.read_csv(SPX_PATH).filter((pl.col("datetime") <= end))
    dates   = df["datetime"]
    i       = bisect_left(dates, start_plus_one)
    
    if dates[i] >= start_plus_one:

        i -= 1

    start   = dates[i]
    df      = df.filter(pl.col("datetime") >= start)

    return df