from    config      import  FUT_DEFS
import  polars      as      pl


pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(-1)


def parse(in_fn: str):

    input   = []
    df      = pl.read_csv(
                in_fn,
                quote_char          = '"',
                infer_schema_length = 0
            ).select(
            [ "DATE", "TIME", "TYPE", "DESCRIPTION" ]
            ).filter(
                pl.col("TYPE") == "TRD"
            )
    symbols = []
    dts     = []
    qtys    = []
    prices  = []
    
    for row in df.iter_rows():

        date    = row[0].split("/")
        date    = f"20{date[2]}-{date[0].zfill(2)}-{date[1].zfill(2)}"
        time    = row[1]
        dt      = f"{date}T{time}"
        desc    = row[3].split()
        err     = False

        try:

            qty     = float(desc[1].replace(",", ""))
            symbol  = desc[2]
            price   = float(desc[3][1:])

            if "/" in symbol:

                # future

                symbol = symbol.split(":")[0][1:-3]

                if FUT_DEFS[symbol]["alias"]:

                    qty    = qty * FUT_DEFS[symbol]["scale"]
                    symbol = FUT_DEFS[symbol]["alias"]
            
            elif len(symbol) > 6:

                err = True
    
        except Exception:

            err = True

        if err:

            '''
            # debug
             
            print(f"skipping: {dt},{desc}")

            dts.append(None)
            qtys.append(None)
            symbols.append(None)
            prices.append(None)
            '''

            err = False
        
        else:

            dts.append(dt)
            qtys.append(qty)
            symbols.append(symbol)
            prices.append(price)

    '''
    # debug

    df = df.with_columns(
            [
                pl.Series("TS", dts),
                pl.Series("QTY", qtys),
                pl.Series("SYMBOL", symbols),
                pl.Series("PRICE", prices)
            ]
        ).drop(
            [ 
                "DATE",
                "TIME",
                "DESCRIPTION", 
                "TYPE" 
            ]
        ).filter(
            pl.col("TS").is_not_null()
        )

    print(df.head(n = 100))

    for symbol in sorted(df["SYMBOL"].unique()):

        print(symbol)
    '''

    input = sorted(
                [ 
                    (
                        symbols[i],
                        dts[i],
                        qtys[i],
                        prices[i]
                    )
                    for i in range(len(dts))
                ],
                key = lambda r: r[1]
            )

    return input