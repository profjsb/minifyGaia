"""
Astrometry centric config file
"""

# what columns do we want to save from the
# the original DB files and what limits do we
# want?
columns_to_pull = [
    ("phot_rp_mean_mag", 12, 19),
    "pmra",
    "pmdec",
    ("parallax", None, 250),
    ("ruwe", None, 1.4),
]
max_pm = 250
drop_missing_rows = True

# list with name, schema, index bool
columns_to_save = [
    ("phot_rp_mean_mag", "decimal(4,2)", False),
    ("pmra", "decimal(6,2)", False),
    ("pmdec", "decimal(6,2)", False),
    ("parallax", "decimal(6,2)", False),
]

default_table_name = "gaiadr3_tiny"
default_filename = "tiny_gaiadf3.db"
default_source_list = "gaia_source.list"
