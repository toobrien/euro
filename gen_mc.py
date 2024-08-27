from    bisect      import bisect_left
from    datetime    import datetime, timedelta
from    os.path     import join
from    parsers     import ninjatrader, tradovate, tradovate_tv, thinkorswim
from    polars      import Config, DataFrame
from    sys         import argv
from    time        import time
from    util        import get_sym_data, in_row


# python gen_mc.py euro_in sc Europe/Berlin tradovate 1

ENABLED = [ "TSLA" ]
PARSERS = { 
            "tradovate":    tradovate,
            "tradovate_tv": tradovate_tv,
            "thinkorswim":  thinkorswim,
            "ninjatrader":  ninjatrader
        }


Config.set_tbl_cols(-1)
Config.set_tbl_rows(-1)


if __name__ == "__main__":

    t0      = time()
    in_fn   = join(".", "csvs", f"{argv[1]}.csv")
    out_fn  = join(".", "csvs", f"{argv[1][:-3]}_out.csv")
    src     = argv[2]
    tz      = argv[3]
    parser  = PARSERS[argv[4]]
    debug   = int(argv[5])
    input   = parser.parse(in_fn)

    if not input:

        print("exiting: no input")

        exit()

    output      = []
    input       = sorted(input, key = lambda r: r[in_row.ts])
    input       = [ row for row in input if row[in_row.symbol] in ENABLED ]
    start       = input[0][in_row.ts].split("T")[0]
    end         = input[-1][in_row.ts].split("T")[0]
    end         = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
    symbols     = set([ row[0] for row in input ])
    sym_data    = get_sym_data(symbols, "0000", "9999", tz, src)
    
    for symbol in symbols:

        # trim and fit to available data

        data            = sym_data[symbol]
        sym_rows        = [ row for row in input if row[in_row.symbol] == symbol ]
        trade_ts        = [ row[in_row.ts] for row in sym_rows ]
        data_ts         = data["ts"]
        data_start      = data_ts[0]
        data_end        = data["ts"][-1]
        i               = bisect_left(trade_ts, data_start)
        j               = bisect_left(trade_ts, data_end)
        sym_rows        = sym_rows[i:j]
        trimmed_start   = sym_rows[0][in_row.ts].split("T")[0]
        trimmed_end     = sym_rows[-1][in_row.ts].split("T")[0]
        trimmed_end     = (datetime.strptime(trimmed_end, "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%d")
        i_              = bisect_left(data_ts, trimmed_start)
        j_              = bisect_left(data_ts, trimmed_end)
        
        for key, val in data.items():

            data[key] = val[i_:j_]

        for row in sym_rows:

            ts      = data["ts"]
            qty     = row[in_row.qty]
            px      = data["close"]
            in_ts   = row[in_row.ts]
            out_idx = bisect_left(ts, in_ts)
            out_ts  = ts[out_idx]
            in_px   = row[in_row.price]
            out_px  = px[out_idx]

            output.append(( symbol, out_ts, out_idx, qty, in_px, out_px ))

    output = sorted(output, key = lambda r: r[in_row.ts])

    with open(out_fn, "w") as fd:

        fd.write("symbol,ts,idx,pos_chg,in_price,out_price\n")

        for line in output:

            fd.write(",".join([ str(i) for i in line ]) + "\n")

    if debug:

        debug_fn    = f"{argv[1][:-3]}_debug.csv"
        df_debug    = DataFrame(
                        {
                            "symbol":   [ row[in_row.symbol]    for row in input ],
                            "ts":       [ row[in_row.ts]        for row in input ],
                            "qty":      [ row[in_row.qty]       for row in input ],
                            "price":    [ row[in_row.price]     for row in input ]
                        }
                    )
        
        df_debug.write_csv(join(".", "csvs", debug_fn))
    
    print(f"{time() - t0:0.1f}s")