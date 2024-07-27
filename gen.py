from    bisect                  import  bisect_left
from    numpy                   import  array
from    os.path                 import  join
from    parsers                 import  tradovate
from    polars                  import  DataFrame, Config
from    sys                     import  argv


# python gen.py euro_in sc Europe/Berlin tradovate 1


PARSERS = { "tradovate": tradovate }


Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    in_fn           = join(".", "csvs", f"{argv[1]}.csv")
    out_fn          = join(".", "csvs", f"{argv[1][:-3]}_out.csv")
    src             = argv[2]
    tz              = argv[3]
    parser          = PARSERS[argv[4]]
    debug           = int(argv[5])
    sym_data, input = parser.parse(in_fn, tz, src)
    mask            = []

    if input:

        input   = sorted(input, key = lambda r: r[1])
        output  = []

        for row in input:

            symbol = row[0]

            if symbol not in sym_data:

                mask.append(0)

                continue

            ts      = sym_data[symbol]["ts"]
            qty     = row[2]
            px      = sym_data[symbol]["close"]
            in_ts   = row[1]

            if in_ts < ts[0] or in_ts > ts[-1]:

                mask.append(0)

                continue
            
            mask.append(1)
            
            out_idx = bisect_left(ts, in_ts)
            out_ts  = ts[out_idx]
            out_px  = px[out_idx]

            output.append(( symbol, out_ts, out_idx, qty, out_px ))

        with open(out_fn, "w") as fd:

            fd.write("symbol,ts,idx,pos_chg\n")

            for line in output:

                fd.write(",".join([ str(i) for i in line[:-1] ]) + "\n")

    if debug:

        input = [ input[i] for i in range(len(input)) if mask[i] ]

        for symbol in sym_data.keys():

            in_rows         = [ row for row in input if row[0] == symbol ]
            in_position     = in_rows[0][2]
            in_pnl          = 0.
            in_pnls         = [ in_pnl ]
            
            out_rows        = [ row for row in output if row[0] == symbol ]
            out_position    = out_rows[0][3]
            out_pnl         = 0.
            out_pnls        = [ out_pnl ]
            
            for i in range(1, len(in_rows)):

                in_pnl          += in_position * (in_rows[i][3] - in_rows[i - 1][3])
                in_position     += in_rows[i][2]
                out_pnl         += out_position * (out_rows[i][4] - out_rows[i - 1][4])
                out_position    += out_rows[i][3]
                
                in_pnls.append(in_pnl)
                out_pnls.append(out_pnl)

            in_pnls         = array(in_pnls)
            out_pnls        = array(out_pnls)
            in_px           = array([ row[3] for row in in_rows ])
            out_px          = array([ row[4] for row in out_rows ])
            diff_px         = out_px - in_px
            diff_pnl        = out_pnls - in_pnls
            diff_pnl_pct    = (out_pnls / in_pnls - 1) * 100

            df = DataFrame(
                    {
                        "symbol":       [ row[0] for row in out_rows ],
                        "in_ts":        [ row[1] for row in in_rows ],
                        "out_ts":       [ row[1] for row in out_rows ],
                        "in_qty":       [ row[2] for row in in_rows ],
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