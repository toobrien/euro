from    enum                    import  IntEnum
from    bisect                  import  bisect_left
from    numpy                   import  array, cumsum
from    os.path                 import  join
from    parsers                 import  tradovate
from    polars                  import  DataFrame, col, Config, Datetime, read_csv
import  plotly.graph_objects    as      go 
from    sys                     import  argv
from    util                    import  adjust_tz, get_sym_data


# python generate.py 20240709_in tradovate ohlcv-1s Europe/Berlin 1


PARSERS     = { "tradovate": tradovate }
DBN_DT_FMT  = "%Y-%m-%dT%H:%M:%S"


Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    in_fn       = join(".", "csvs", f"{argv[1]}.csv")
    out_fn      = join(".", "csvs", f"{argv[1][:-3]}_out.csv")
    parser      = PARSERS[argv[2]]
    schema      = argv[3]
    tz          = argv[4]
    debug       = int(argv[5])

    symbols, input, output = parser.parse(in_fn, schema, tz, DBN_DT_FMT)

    if output:

        output  = sorted(output, key = lambda r: r[1])
        input   = sorted(input, key = lambda r: r[1])

        with open(out_fn, "w") as fd:

            fd.write("symbol,ts,idx,pos_chg\n")

            for line in output:

                fd.write(",".join([ str(i) for i in line[:-1] ]) + "\n")

    if debug:

        for symbol in symbols:

            in_rows         = [ row for row in input if row[0] == symbol ]
            in_position     = in_rows[0][3]
            in_pnl          = 0.
            in_pnls         = [ in_pnl ]
            
            out_rows        = [ row for row in output if row[0] == symbol ]
            out_position    = out_rows[0][3]
            out_pnl         = 0.
            out_pnls        = [ out_pnl ]
            
            for i in range(1, len(in_rows)):

                in_pnl          += in_position * (in_rows[i][4] - in_rows[i - 1][4])
                in_position     += in_rows[i][3]
                out_pnl         += out_position * (out_rows[i][4] - out_rows[i - 1][4])
                out_position    += out_rows[i][3]
                
                in_pnls.append(in_pnl)
                out_pnls.append(out_pnl)

            in_pnls         = array(in_pnls)
            out_pnls        = array(out_pnls)
            in_px           = array([ row[4] for row in in_rows ])
            out_px          = array([ row[4] for row in out_rows ])
            diff_px         = out_px - in_px
            diff_pnl        = out_pnls - in_pnls
            diff_pnl_pct    = (out_pnls / in_pnls - 1) * 100

            df = DataFrame(
                    {
                        "symbol":       [ row[0] for row in out_rows ],
                        "in_ts":        [ row[1] for row in in_rows ],
                        "out_ts":       [ row[1] for row in out_rows ],
                        "in_qty":       [ row[3] for row in in_rows ],
                        "out_qty":      [ row[3] for row in out_rows ],
                        "in_px":        in_px,
                        "out_px":       out_px,
                        "diff_px":      diff_px,
                        "in_pnl":       in_pnls,
                        "out_pnl":      out_pnls,
                        "diff_pnl":     diff_pnl,
                        "diff_pnl (%)": diff_pnl_pct
                    }
                )

            print(df)