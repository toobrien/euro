from    bisect  import  bisect_left
from    os.path import  join
from    parsers import  ninjatrader, tradovate, thinkorswim
from    polars  import  Config, DataFrame
from    sys     import  argv
from    time    import  time


# python gen.py euro_in sc Europe/Berlin tradovate 1


PARSERS = { 
            "tradovate":    tradovate,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    t0              = time()
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
            px      = sym_data[symbol]["open"]
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

            fd.write("symbol,ts,idx,pos_chg,price\n")

            for line in output:

                fd.write(",".join([ str(i) for i in line ]) + "\n")

    if debug:

        input       = [ input[i] for i in range(len(input)) if mask[i] ]
        debug_fn    = f"{argv[1][:-3]}_debug.csv"
        df_debug    = DataFrame(
                        {
                            "symbol":   [ row[0] for row in input ],
                            "ts":       [ row[1] for row in input ],
                            "qty":      [ row[2] for row in input ],
                            "price":    [ row[3] for row in input ]
                        }
                    )
        
        df_debug.write_csv(join(".", "csvs", debug_fn))
    
    print(f"{time() - t0:0.1f}s")