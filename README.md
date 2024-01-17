# Carto Data Processing

## Requirements

Make sure you have install `python3.10`.

## Enviromental Variables

**Mapbox Access Token:**
The script also use Mapbox platform for some geoprocessing, to acquire the token doing the following:

1. Register a mapbox account: [Link](https://account.mapbox.com/auth/signup/).
2. Go to the access tokens management page: [Link](https://account.mapbox.com/access-tokens/)
3. Copy either the default access token or generate a specific token as shown in these [instructions](https://docs.mapbox.com/help/getting-started/access-tokens/).

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
python process_services.py ~/Caracterización_de_punto_de_servicio_-_Aurora_Office_-_Ronda_2_-_all_versions_-_False_-_2023-11-28-13-59-47.csv --output ~/output --format csv
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
python process_aurora.py --cara_path "~/Aurora v2.1 data file - caracterización.csv" --ayuda_path "~/Aurora v2.1 data file - ayudaHumanitaria.csv" --output ~/aurora_round_2 --format csv
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
python process_monitoreos.py --cara_path path/to/file --ayuda_path path/to/file --mon_path path/to/file --output ~/aurora_round_2_31102023 --format csv
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
python process_aurora.py --cara_path "~/Aurora v2.1 data file - caracterización.csv" --ayuda_path "~/Aurora v2.1 data file - ayudaHumanitaria.csv" --mon_path "~/Aurora v2.1 data file - monitoreo.csv" --output "Carto_map" --format csv
```

## `process_feedback`

The first part of this script is similar to "process_monitoreos"; however, while process_monitoreos was thinking to generate the structure of geographical variables for Caro map, this script is focus on the generation of a complete data base and the necessary transformations for feedback pages in Carto.  

The steps are:

1. Make sure the required packages are install.
2. Retrieve data from google drive folder. The google sheet document named: `Aurora v2.1 data file`. Export the required sheets which include:

   - **caracterización** export in csv as `Aurora v2.1 data file - caracterización.csv`
   - **ayudaHumanitaria** export in csv as `Aurora v2.1 data file - ayudaHumanitaria.csv`
   - **monitoreo** export in csv as `Aurora v2.1 data file - monitoreo.csv`
   - **info** export in csv as `Aurora v2.1 data file - solicitudInformación.csv`

3. Run the script in your terminal. The following option are available:

```Bash
python process_feedback.py --cara_path path/to/file --ayuda_path path/to/file --mon_path path/to/file --info_path path/to/file --output "~\feedback, ~\feedback_nna" --format csv
```

The option available:

- **--cara_path:** File location path for Aurora Characterization data in csv format
- **--ayuda_path:** File location path for Aurora Feedback data in csv format
- **--mon_path:** File location path for Aurora Monitoring data in csv format
- **--info_path:** File location path for Aurora Information selection from migrants data   in csv format
- **--destination:** Carto data warehouse endpoint
- **--output:** Output name or output path (separated by comma)
- **--format:** Output format if output path is given. Can either be `csv` or `json`.

**Example:**

```Bash
python process_feedback.py --cara_path "~\Aurora v2.1 data file - caracterización.csv" --ayuda_path "~\Aurora v2.1 data file - ayudaHumanitaria.csv" --mon_path "~\Aurora v2.1 data file - monitoreo.csv" --info_path "~\Aurora v2.1 data file - solicitudInformación.csv" -  --output "~\feedback, ~\feedback_nna" --format csv
```

This script generates files needed in Carto (feedback) and the complete base with all variables

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

## Other Documentation

- [`Modules`](/modules/README.md): Directory of custom functions which are used in scripts.
- [`Defaults`](/defaults/README.md): Directory of JSON objects of constant values that which are used in scripts.
