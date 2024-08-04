from    config      import  TS_FMT
from    datetime    import  datetime, timedelta
import  polars      as      pl
from    sys         import  path

path.append(".")

from util       import get_sym_data

pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(-1)


def parse(
    in_fn:      str,
    tz:         str,
    src:        str
):
    
    pass