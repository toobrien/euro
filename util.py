import polars as pl


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