from    os.path     import  join
import  polars      as      pl
from    sys         import  argv, path


path.append(".")
pl.Config.set_tbl_cols(-1)


# python parsers/concat_tos_futs.py tydal_futs tydal_in


if __name__ == "__main__":

    futs_fn = join(".", "csvs", f"{argv[1]}.csv")
    in_fn   = join(".", "csvs", f"{argv[2]}.csv")
    out_fn  = join(".", "csvs", f"{argv[2]}_concat.csv")
    futs_df = pl.read_csv(futs_fn, quote_char = '"', infer_schema_length = 0)
    in_df   = pl.read_csv(in_fn, quote_char = '"', infer_schema_length = 0)
    futs_df = futs_df.drop("Trade Date")
    futs_df = futs_df.rename(
                                # futs: Trade Date,Exec Date,Exec Time,Type,Ref #,Description,Misc Fees,Commissions & Fees,Amount,Balance
                                # in:   DATE,TIME,TYPE,REF #,DESCRIPTION,Misc Fees,Commissions & Fees,AMOUNT,BALANCE
                                {
                                    "Exec Date":    "DATE",
                                    "Exec Time":    "TIME",
                                    "Type":         "TYPE",
                                    "Ref #":        "REF #",
                                    "Description":  "DESCRIPTION",
                                    "Amount":       "AMOUNT",
                                    "Balance":      "BALANCE"
                                }
                            )
    out_df  = in_df.vstack(futs_df)

    print(in_df.head())
    print(futs_df.head())
    print(out_df)

    out_df.write_csv(out_fn)