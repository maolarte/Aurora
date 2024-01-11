# Carto Data Processing

## Requirements

Make sure you have install `python3.10`.

The script also use Mapbox platform for some geoprocessing, as such a register a mapbox account and generate a token as shown in these [instructions](https://docs.mapbox.com/help/getting-started/).

After acquiring the access token, add it as part of your environmental variables:

```Bash
$ export MAPBOX_TOKEN=eh.pk....
```

## How to run the scripts

Before running the scripts, you need to install the require modules which have been the `requirements.txt`.

```Bash
cd path/to/repo
$ python -m venv ./env
$ ./env/Scripts/activate
$ pip install -r ./requirements.txt
```

### `process_services`

This script process data collected using Kobo.

The steps are:

1. Make sure the required packages are install
2. Retrieve data from KoboToolbox, of which the naming converson is as follows:

   `Caracterización_de_punto_de_servicio_-_Aurora_Office_-_Ronda_2_-_all_versions_-_False_-_[EXPORTED DATE].csv`

3. Run the script in your terminal. The following option are available:

```Bash
python process_services.py  {path/to/file} --output path/to/file --format csv
```

The options available:

- **{path/to/file}:** File path, the location of which the service data file is located. Also accepts https file endpoints.
- **--destination:** Carto data warehouse endpoint
- **--output:** Output name or output path
- **--format:** Output format if output path is given. Can either be `csv` or `json`.

**Example:**

```Bash
python process_services.py ~/Caracterización_de_punto_de_servicio_-_Aurora_Office_-_Ronda_2_-_all_versions_-_False_-_2023-11-28-13-59-47.csv --output ~/services_round_2 --format csv
```

### `process_aurora`

This script process data collected using Landbot. It uses two data sheets

- Feedback Data
- Characterization Data

The steps are:

1. Make sure the required packages are install
2. Retrieve data from google drive folder. The google sheet document named: `Aurora v2.1 data file`. Export the required sheets which include:

   - **caracterización** export in csv as `Aurora v2.1 data file - caracterización.csv`
   - **ayudaHumanitaria** export in csv as `Aurora v2.1 data file - ayudaHumanitaria.csv`

3. Run the script in your terminal.

```Bash
python process_aurora.py --cara_path path/to/file --ayuda_path path/to/file --output path/to/file --format csv
```

The option available:

- **--cara_path:** File location path to `Aurora v2.1 data file - caracterización.csv`
- **--ayuda_path:** File location path to `Aurora v2.1 data file - ayudaHumanitaria.csv`
- **--destination:** Carto data warehouse endpoint
- **--output:** Output name or output path
- **--format:** Output format if output path is given. Can either be `csv` or `json`.

**Example:**

```Bash
python process_aurora.py --cara_path "~/Aurora v2.1 data file - caracterización.csv" --ayuda_path "~/Aurora v2.1 data file - ayudaHumanitaria.csv" --output services_round_2 --format csv
```

## `process_monitoreos`

The steps are:

1. Make sure the required packages are install.
2. Retrieve data from google drive folder. The google sheet document named: `Aurora v2.1 data file`. Export the required sheets which include:

   - **caracterización** export in csv as `Aurora v2.1 data file - caracterización.csv`
   - **ayudaHumanitaria** export in csv as `Aurora v2.1 data file - ayudaHumanitaria.csv`
   - **monitoreo** export in csv as `Aurora v2.1 data file - monitoreo.csv`

3. Run the script in your terminal. The following option are available:

```Bash
python process_monitoreos.py --cara_path path/to/file --ayuda_path path/to/file --mon_path path/to/file --output aurora_round_2 --format csv
```

The option available:

- **--cara_path:** File location path for Aurora Characterization data in csv format
- **--ayuda_path:** File location path for Aurora Feedback data in csv format
- **--mon_path:** File location path for Aurora Monitoring data in csv format
- **--destination:** Carto data warehouse endpoint
- **--output:** Output name or output path
- **--format:** Output format if output path is given. Can either be `csv` or `json`.

**Example:**

```Bash
python process_aurora.py --cara_path "~/Aurora v2.1 data file - caracterización.csv" --ayuda_path "~/Aurora v2.1 data file - ayudaHumanitaria.csv" --mon_path "~/Aurora v2.1 data file - monitoreo.csv" --output aurora_monitoring_around_2 --format csv
```

### How to load data to Carto Data Warehouse

The data processing scripts have two ways to handle the processed data, either export it as file (`csv` or `json`) and manually upload the file to Carto or upload direct to Carto Platform.

1. **Manual Upload**

You need to run a script adding the option `--output` which gives the output location for procesed data. By default the format will be in csv but you can also get json format by adding the `--format` option:

**Example:**

```Bash
python process_service.py ~/raw_data.csv --output ~/output --format json
```

once you have the file ready, you can follow these steps to upload to carto data warehouse: [link](https://docs.carto.com/carto-user-manual/data-explorer/importing-data#how-to-import-data)

2. **Carto Data Client Upload**

You can also upload the data directly to carto data warehouse. You can use `--destination` flag to add your destination in carto using bigquery destination notation.

**Example:**

```Bash
python process_service.py ~/raw_data.csv --destination carto-dw-ac-4v8fnfsh.shared.test
```
