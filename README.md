# DBTruck
[Eugene Wu](http://www.sirrice.com)

The current database import process is like a toilet pipe.  The pipe
easily gets clogged if your data is a bit dirty.  You gotta clean up the data, figure out the schema, figure out the types, etc etc.  It really sucks!  I once spent 2.5 hours importing two datasets from the [FEC](http://www.fec.gov/disclosurep/pnational.do).

`DBTruck` is meant to turn the import process into a dump truck.  Just throw data
into it, should just work!  Who cares about attribute names!  Who cares about types you don't care about!  You can clean it up later!

It assumes that your file is one tuple per line.  Other than that, it
will:

* Automatically split up each line in a way that makes sense
* Try to interpret each column's type
  * Currently supports `int`, `float`, `date`, `time`, `timestamp`
  * Defaults to `varchar(100)`
* Make fake attribute names (`attr0`,…,`attrN`) so you don't need to
* Import the file in blocks so that a single bad row doesn't blow the
  whole operation
  * It'll even pinpoint the specific bad rows and log them to an error
    file 


`DBTruck` assumes that `PostgreSQL` is installed and running on your 
machine, and uses `psql` to load data.

# Requirements

* Python
* [dateutil](http://labix.org/python-dateutil#head-2f49784d6b27bae60cde1cff6a535663cf87497b)
* [openpyxl](http://packages.python.org/openpyxl/usage.html)
* [PostgreSQL](http://www.postgresql.org/download/) running locally
* psql


# Installation

The installation should install an executable called `importmydata.py` into your path:

    python setup.py install

# Usage

    importmydata.py -h
    importmydata.py data/testfile.txt tablename dbname

# TODOs

Immediate ToDos

* ~~Faster import: lots of datafiles have errors scattered throughout the data, which dramatically
  slows down bulk inserts.~~ 
  * ~~Do preliminary filtering for errors~~
  * ~~Fall back to (prepared) individual inserts once too many bulk insert attempts fail~~
* Better error reporting
  * Load failed data into a hidden table in the database
  * Log error reasons
  * Try to recover from typical errors (date column contains a random string) by using reasonable defaults
* ~~Refactor file iterator objects to keep track of hints identified earlier in the pipeline~~
  * for example, parsed json files can infer that the dictionary keys are table headers -- no
    need to re-infer that later in the pipeline
  * Include confidence scores for each inference
* ~~Support extracting multiple tables from each input file~~
  * an HTML file may contain multiple tables to be imported
* ~~Support downloading URLS and HTML files~~
* ~~Support CSV output~~
* ~~Support Excel Files~~


## Comments

If there are uses you would like to see, let me know!  I'm adding features for what
I want, but I'm interested in other uses.

In the future I would like to add

* Good geocoder for location columns
  * support for simple location joins
* support for other databases
* let you specify port/host etc
* support additional data file types (json, fixed offset, serialized)
* support renaming and reconfiguring the tables after the fact
* inferring foreign key relationships
* creating indexes
* interactive interface instead of requiring command line flags
* and more!
