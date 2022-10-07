#!#!/usr/bin/env python
"""
minifyGaia

Code to lossy compress Gaia into a single duckDB
database for specific purposes, such as serving as
the backbone for a finding chart generator.

Support for ADQL queries is provided through HTM indexing.

Command-line usage examples:

  # make the database, overwriting what's there
  # already if need be
  python ./minifyGaia.py make_db --clobber=True
  
  # load a specific file into the DB
  # should take < 10 seconds on a modern machine
  python ./minifyGaia.py add_file <filename>

  # load the entire dataset into the DB
  # using the file `gaia_source.list`
  # this will take of order 24 hours if
  # you do not have the full DB downloaded
  python ./minifyGaia.py ingest_all --source_list=gaia_source.list

"""
import os
import time
from subprocess import call
import datetime

import pandas as pd
import numpy as np

import fire
import duckdb

# pull in the appropriate config file for the task
# at hand. Change this file if you wish.
from config_astrom import *

# always pull these columns
columns_to_save = [("htm20", "UBIGINT NOT NULL", True), 
                   ("x", "DOUBLE NOT NULL", False),
                   ("y", "DOUBLE NOT NULL", False),
                   ("z", "DOUBLE NOT NULL", False)] + columns_to_save

columns_to_pull = list(set(columns_to_pull + ["ra", "dec"]))

def make_db(table_name = default_table_name, 
            filename=default_filename, clobber=False):
    
    if clobber and os.path.exists(filename):
        os.remove(filename)
        print(f"removed file {filename}")

    conn = duckdb.connect(filename)
    sql_create_table = f"""CREATE TABLE {table_name} (\n"""
    for column in columns_to_save:
        sql_create_table += f"{column[0]} {column[1]},\n"
    sql_create_table += ")"

    sql_create_meta = """
            CREATE TABLE meta (  
            filename STRING,
            nrows INTEGER,
            nnows_original INTEGER,
            time_to_add float,
            datetime_added timestamp default current_timestamp,
            )"""

    conn.execute(sql_create_table)
    conn.execute(sql_create_meta)

    for column in columns_to_save:
        if column[2]:
            print(f"Creating index on {column[0]}")
            sql_create_indices =  f"""
                CREATE INDEX {column[0]}_index ON {table_name} ({column[0]});"""
            conn.execute(sql_create_indices)

    print(f"DB created in file {filename}")


def add_file(fname, table_name = default_table_name, 
            filename=default_filename):

    conn = duckdb.connect(filename)
    
    rez = conn.execute(f"""SELECT * from meta where filename = '{fname}'""").fetchall()
    if len(rez) != 0:
        print(f"[{fname}] Already added on {rez[0][4]}. Skipping.")
        return

    unz_fname = fname if fname.find(".gz") == -1 else fname.split(".gz")[0]

    start = time.time()
    small_fname = f"sm{unz_fname}"
    indexed_fname = f"in{unz_fname}"
    small_indexed_fname = f"sin{unz_fname}"

    colnames = [x if isinstance(x, str) else x[0] for x in columns_to_pull]
    df = pd.read_csv(f"{fname}", 
                comment="#", usecols=colnames)
    
    if drop_missing_rows:
        df1 = df.dropna(axis=0, how="any")
    else:
        df1 = df.copy()

    # use the bounds on what we ingest 
    # to create a mask
    mask = np.ones((len(df1),), dtype=bool)
    for column in columns_to_pull:
        if isinstance(column, str):
            continue
        if column[1] is not None:
            mask = mask & (df1[column[0]] >= column[1])
        if column[2] is not None:
            mask = mask & (df1[column[0]] < column[2])

    if max_pm is not None:
        if "pmra" not in columns_to_pull or "pmdec" not in columns_to_pull:
            raise KeyError("Missing required columns to limit proper motion") 
        mask = mask & (df1.loc[:,"pmra"]**2 + df1.loc[:, "pmdec"]**2 < max_pm**2)
    
    # apply the mask
    df1 = df1[mask]
    print(f"[{fname}] With cuts, compressed to {100*len(df1)/len(df):4.2f}% of original # rows")

    df1.to_csv(small_fname, index=False)
    _ = call(f"SpatialIndex/bin/sptIndx 20 {small_fname} {indexed_fname}", shell=True)

    colnames = [x if isinstance(x, str) else x[0] for x in columns_to_save]
    pd.read_csv(f"{indexed_fname}",usecols=colnames)[colnames] \
                    .to_csv(small_indexed_fname, index=False)
    
    conn.execute(f"""COPY {table_name} FROM '{small_indexed_fname}' ( HEADER,  SAMPLE_SIZE 1, AUTO_DETECT FALSE );""")
    total = time.time() - start

    # save some bookkeeping data
    q = f"""
        INSERT INTO meta VALUES ('{fname}', {len(df1)}, {len(df)}, {total}, '{datetime.datetime.utcnow()}');
        """
    conn.execute(q)

    # shutdown
    conn.close()
    os.remove(small_fname)
    os.remove(indexed_fname)
    os.remove(small_indexed_fname)

def ingest_all(source_list=default_source_list, download_if_missing=True,
               loc="http://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source"):

    for line in open(source_list, "r").readlines():
        fname = line.split()[0]
        if not os.path.exists(fname) and download_if_missing:
            print(f"Downloading {fname}")
            _ = call(f"wget {loc}/{fname}", shell=True)

        if os.path.exists(fname):
            add_file(fname)

if __name__ == '__main__':
    fire.Fire({
        'add_file': add_file,
        'make_db': make_db,
        'ingest_all': ingest_all,
    })
