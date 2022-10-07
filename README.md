# minifyGaia
Code to shrink Gaia down to a single DB, for specific purposes (eg. astrometry)

## Installation

Install the Python requirements:

```
pip install -r requirements.txt
```

Install the indexing requirements: first make sure that you have `make` and `gcc` installed and in your path. Then:

```
git clone https://github.com/Caltech-IPAC/SpatialIndex.git
cd SpatialIndex
make
python setup.py install
```

You'll now have the indexing executable `SpatialIndex/bin/sptIndx` in your path and the Python `spatial-index` installed.

## Build the Database

First, edit the `config_astrom.py` file to make sure that you are setting the parameters you want to save in the minified DB and any boundaries on parameters that you want to enforce (e.g. mag < 18.0).

Make the DB:

```
python ./minifyGaia.py make_db --clobber=True
```

Ingest the DB:

```
python ./minifyGaia.py ingest_all --source_list=gaia_source.list
```

This will download the Gaia DR3 *.csv.gz files as they load into the DB.

## Query the DB using ADQL

In Python, connect to the database:

```python
import duckdb
from config_astrom import default_filename, default_table_name

conn = duckdb.connect(default_filename, read_only=True)
```

Let's say we want to issue a query around a certain point:

```python
import healpy
from ADQL.adql import ADQL
from spatial_index import SpatialIndex
from healpy.pixelfunc import vec2ang
import numpy as np
from astropy.table import Table

ra_source, dec_source = 45.00432028915398, 0.0210477637811747
max_distance_deg = 0.05
 
adql_string = f"""
   SELECT x, y, z, phot_rp_mean_mag, pmra, pmdec, parallax
   FROM {default_table_name}
   WHERE 1=CONTAINS(
      POINT('ICRS', ra, dec),
      CIRCLE('ICRS', {ra_source} , {dec_source}, {max_distance_deg}))
      
adql = ADQL(dbms='sqlite', level=20, 
                racol='ra', deccol='dec',
                xcol = 'x', ycol='y', zcol='z', indxcol='htm20',
                mode=SpatialIndex.HTM, encoding=SpatialIndex.BASE10)
q = adql.sql(adql_string)
```

Now we have our ADQL query converted into a duckDB query. Get the results and add in ra dec colunms

```python
ret = conn.execute(q).fetchall()
radec = np.array([vec2ang(np.array([row[0], row[1], row[2]]),lonlat=True) for row in ret]).squeeze()
df = pd.DataFrame(ret, columns = ["x", "y", "z",
                    "phot_rp_mean_mag", "pmra", "pmdec", "parallax"])
df["ra"], df["dec"] = radec[:,0] ,radec[:,1]
```

Now we can filter this as an astropy Table, sorting by distance from the source position:

```python
import astropy.units as u
from astropy.coordinates import SkyCoord

c1 = SkyCoord(ra_source*u.deg, dec_source*u.deg)
c2 = SkyCoord(df["ra"]*u.deg, df["dec"]*u.deg)

df["dist"] = c1.separation(c2).arcsec
del df["x"]
del df["y"]
del df["z"]

r = Table.from_pandas(df)
filter_mask = (
        (r["phot_rp_mean_mag"] < 18)
        & (r["phot_rp_mean_mag"] > 10)
        & (r["parallax"] < 250)
    )
r[filter_mask]
r.sort("dist")