# Files connected to Carto

There are three main scripts for processing data for Carto

1. `process_services.py`:

   - This script process data collected using Kobo.

2. `process_aurora.py`:

   - This script process data collected using Landbot. It uses three data sheets
     - ayudaHumanitaria
     - caracterización
     - monitoreo

3. `process_monitoreps.py`:
   - This script process data collected using Landbot. It uses three data sheets
     - ayudaHumanitaria
     - caracterización
     - monitoreo

The final procedure of script is to upload the processed data to Carto DataWarehouse.

## How to run the scripts

Before running the scripts, you need to install the require modules which have been the `requirements.txt`.

```Bash
cd path/to/repo
$ python -m venv ./env
$ ./env/Scripts/activate
$ pip install -r ./requirements.txt
```

### `process_services`

The steps are:

1.
2.

An example to run it ois:

```Bash
cd path/to/repo
$ cp source/file target/path
$ pip install...
$ python process_services.py path/to/file
```

After that, upload the output files to...

### `process_aurora`

The steps are:

1.
2.

An example to run it is:

```Bash
cd path/to/repo
$ cp source/file target/path
$ pip install...
$ python process_aurora.py path/to/file
```

After that, upload the output files to...

## `process_monitoreps`

The steps are:

1.
2.

An example to run it is:

```Bash
cd path/to/repo
cp source/file target/path
pip install...
python process_monitorepss.py path/to/file
```

After that, upload the output files to...

## Tests

TBD...
