from os.path import join


TS_FMT              = "%Y-%m-%dT%H:%M:%S"
DBN_PATH            = join("..", "databento", "csvs" )
SC_PATH             = join("/", "Volumes", "[C] Windows 11", "SierraChart", "Data")
SC_TZ               = "America/Los_Angeles"
STOCK_ENABLED       = False
STOCK_MULTIPLIER    = 1.
FUT_DEFS            = {
                        "ES":   { "multiplier": 50.,        "enabled": True,    "alias": None,  "scale": 1.0 },
                        "NQ":   { "multiplier": 20.,        "enabled": True,    "alias": None,  "scale": 1.0 },
                        "YM":   { "multiplier": 5.,         "enabled": False,   "alias": None,  "scale": 1.0 },
                        "RTY":  { "multiplier": 50.,        "enabled": False,   "alias": None,  "scale": 1.0 },
                        "CL":   { "multiplier": 1_000.,     "enabled": False,   "alias": None,  "scale": 1.0 },
                        "NG":   { "multiplier": 10_000.,    "enabled": False,   "alias": None,  "scale": 1.0 },
                        "MES":  { "multiplier": 5.,         "enabled": False,   "alias": "ES",  "scale": 0.1 },
                        "MNQ":  { "multiplier": 2.,         "enabled": False,   "alias": "NQ",  "scale": 0.1 },
                        "M2K":  { "multiplier": 0.50,       "enabled": False,   "alias": "RTY", "scale": 0.1 },
                        "MYM":  { "multiplier": 0.50,       "enabled": False,   "alias": "YM",  "scale": 0.1 },
                        "MCL":  { "multiplier": 1.,         "enabled": False,   "alias": "CL",  "scale": 0.1 }
                    }