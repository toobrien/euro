from os.path import join


TS_FMT      = "%Y-%m-%dT%H:%M:%S"
DBN_PATH    = join("..", "databento", "csvs" )
SC_PATH     = join("/", "Volumes", "[C] Windows 11", "SierraChart", "Data")
SC_TZ       = "America/Los_Angeles"
FUT_DEFS    = {
                "ES":   { "multiplier": 50.,        "enabled": False,   "alias": None   },
                "NQ":   { "multiplier": 20.,        "enabled": True,    "alias": None   },
                "YM":   { "multiplier": 5.,         "enabled": False,   "alias": None   },
                "RTY":  { "multiplier": 50.,        "enabled": False,   "alias": None   },
                "CL":   { "multiplier": 1_000.,     "enabled": False,   "alias": None   },
                "NG":   { "multiplier": 10_000.,    "enabled": False,   "alias": None   },
                "MES":  { "multiplier": 5.,         "enabled": False,   "alias": "ES"   },
                "MNQ":  { "multiplier": 2.,         "enabled": False,   "alias": "NQ"   },
                "M2K":  { "multiplier": 0.50,       "enabled": False,   "alias": "RTY"  },
                "MYM":  { "multiplier": 0.50,       "enabled": False,   "alias": "YM"   },
                "MCL":  { "multiplier": 1.,         "enabled": False,   "alias": "CL"   }
            }