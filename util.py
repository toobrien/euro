from    bisect      import  bisect_left
from    datetime    import  datetime
from    config      import  DBN_PATH, FUT_DEFS, SC_PATH, SC_TZ, SPX_PATH, TS_FMT
from    enum        import  IntEnum
from    numpy       import  array, corrcoef, mean, sqrt, var
from    os.path     import  join, normpath
import  polars      as      pl
from    pytz        import  timezone
from    scipy.stats import  norm
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
    

    suffix  = ".scid_BarData.txt" if not daily else ".dly_BarData.txt"
    fn      = join(SC_PATH, f"{symbol}{suffix}")
    fd      = open(fn, "r") # polars can't read fn for some reason
    df      = pl.read_csv(
                source              = fd,
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


def get_benchmark(symbol, start, end, tz):

    if symbol == "SPX":

        df = pl.read_csv(SPX_PATH).filter((pl.col("datetime") <= end))

    else:

        # sc

        df = get_sc_df(symbol, tz, 1).drop([ "date", "time", " Volume", " OpenInterest" ])
        df = df.rename({ "ts": "datetime" }).filter(pl.col("datetime") <= end)

    dates   = df["datetime"]
    i       = bisect_left(dates, start)
        
    if dates[i] >= start:

        i -= 1
        
    start   = dates[i]
    df      = df.filter(pl.col("datetime") >= start)

    return df


def get_spx(start, end):

    # this version prepends an additional day to the benchmark because ???

    df      = pl.read_csv(SPX_PATH).filter((pl.col("datetime") <= end))
    dates   = df["datetime"]
    i       = bisect_left(dates, start)
    
    if dates[i] >= start:

        i -= 1

    start   = dates[i]
    df      = df.filter(pl.col("datetime") >= start)

    return df


def sharpe_htest(a: array, b: array, rfr: float, alpha: float):

    T               = a.shape[0]

    mu_a            = mean(a)
    var_a           = var(a, ddof = 1)
    sigma_a         = sqrt(var_a)
    sr_a            = (mu_a - rfr) / sigma_a
    skew_a          = ((T - 2) / sqrt(T * (T - 1))) * (((T * sum(a**3) - 3 * sum(a) * sum(a**2) + 2 * sum(a)**3 / T) / ((T - 1) * (T - 2)))) / sigma_a**3
    kurt_a          = 3 * (T - 1) / (T + 1) + (((T - 2) * (T - 3)) / ((T + 1) * (T - 1))) * (((T**3 + T**2) * sum(a**4) - 4 * (T**2 + T) * sum(a**3) * sum(a) - 3 * (T**2 - T) * sum(a**2)**2 + 12 * T * sum(a**2) * sum(a)**2 - 6 * sum(a)**4) / (var_a**2 * T * (T - 1) * (T - 2) * (T - 3)) )

    mu_b            = mean(b)
    var_b           = var(b, ddof = 1)
    sigma_b         = sqrt(var_b)
    sr_b            = (mu_b - rfr) / sigma_b
    skew_b          = ((T - 2) / sqrt(T * (T - 1))) * (((T * sum(b**3) - 3 * sum(b) * sum(b**2) + 2 * sum(b)**3 / T) / ((T - 1) * (T - 2)))) / sigma_b**3
    kurt_b          = 3 * (T - 1) / (T + 1) + (((T - 2) * (T - 3)) / ((T + 1) * (T - 1))) * (((T**3 + T**2) * sum(b**4) - 4 * (T**2 + T) * sum(b**3) * sum(b) - 3 * (T**2 - T) * sum(b**2)**2 + 12 * T * sum(b**2) * sum(b)**2 - 6 * sum(b)**4) / (var_b**2 * T * (T - 1) * (T - 2) * (T - 3)))

    #corr_ab         = corrcoef(a, b, ddof = 1)[0, 1] # ddof deprecated, no effect
    corr_ab         = corrcoef(a, b)[0, 1]
    u_2a_2b         = (-3 * sum(b)**2 * sum(a)**2 + T * sum(b**2) * sum(a)**2 + 4 * T * sum(b) * sum(a) * sum(a * b) - 2 * (2 * T - 3) * sum(a * b)**2 - 2 * (T**2 - 2 * T + 3) * sum(a) * sum(a * b**2) + sum(b)**2 * sum(a**2) - (2 * T - 3) * sum(b**2) * sum(a**2) - 2 * (T**2 - 2 * T + 3) * sum(b) * sum(a**2 * b) + T * (T**2 - 2 * T + 3) * sum(a**2 * b**2)) / (T * (T - 1) * (T - 2) * (T - 3))
    u_1a_2b         = (2 * sum(b)**2 * sum(a) - T * sum(b**2) * sum(a) - 2 * sum(b) * sum(a * b) + T**2 * sum(a * b**2)) / (T * (T - 1) * (T - 2))
    u_1b_2a         = (2 * sum(a)**2 * sum(b) - T * sum(a**2) * sum(b) - 2 * sum(a) * sum(a * b) + T**2 * sum(a**2 * b)) / (T * (T - 1) * (T - 2))
    var_sr_a        = 1 + sr_a**2 / 4 * (kurt_a - 1) - sr_a * skew_a
    var_sr_b        = 1 + sr_b**2 / 4 * (kurt_b - 1) - sr_b * skew_b
    cov_sr_ab       = (
                        corr_ab + (sr_a * sr_b / 4) * (u_2a_2b / (var_a * var_b) - 1) -
                        0.5 * sr_a * u_1b_2a / (sigma_b * var_a) - 
                        0.5 * sr_b * u_1a_2b / (sigma_a * var_b)
                    )
    var_diff        = var_sr_a + var_sr_b - 2 * cov_sr_ab
    sigma_diff      = sqrt(var_diff / (T - 1))
    sr_a_bc         = sr_a / (1 + 0.25 * (kurt_a - 1) / T)
    sr_b_bc         = sr_b / (1 + 0.25 * (kurt_b - 1) / T)
    sr_diff         = sr_b_bc - sr_a_bc

    ub_sr_diff_eq_0     = norm.ppf(1 - alpha / 2, 0, sigma_diff)
    lb_sr_diff_eq_0     = norm.ppf(alpha / 2, 0, sigma_diff)
    ub_sr_diff_lte_0    = norm.ppf(1 - alpha, 0, sigma_diff)
    p_sr_diff_lte_0     = 1 - norm.cdf(sr_diff / sigma_diff)
    p_sr_diff_eq_0      = p_sr_diff_lte_0 * 2

    res                 = {
                            "p_eq_0":       p_sr_diff_eq_0,
                            "p_b_lt_a":     p_sr_diff_lte_0,
                            "sr_diff":      sr_diff
                        }

    return res