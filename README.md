# Carto Data Processing

## Requirements

Make sure you have install `python3.10`.

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
2. Run the script in your terminal. The following option are available:

```Bash
python process_services.py  {path/to/file} --output_path path/to/file --format csv
```

The options available:

- `{path/to/file}`: File path, the location of which the service data file is located. Also accepts https file endpoints.
- `--destination`: Carto data warehouse endpoint
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.
  An example to run it is:

### `process_aurora`

This script process data collected using Landbot. It uses two data sheets

- Feedback Data
- Characterization Data

The steps are:

1. Make sure the required packages are install
2. Run the script in your terminal.

- **Example:**

```Bash
python process_aurora.py --cara_path path/to/file --feedback_path path/to/file --output_path path/to/file --format csv
```

The option available:

- `--cara_path`: File location path for Aurora Characterization data in csv format
- `--feedback_path`: File location path for Aurora Feedback data in csv format
- `--destination`: Carto data warehouse endpoint
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.

## `process_monitoreos`

This script process data collected using Landbot. It uses three data sheets

- Feedback Data
- Characterization Data
- Monitoring Data

The steps are:

1. Make sure the required packages are install
2. Run the script in your terminal. The following option are available:

```Bash
python process_monitoreos.py --cara_path path/to/file --feedback_path path/to/file --monitero_path path/to/file --output_path path/to/file --format csv
```

The option available:

- `--cara_path`: File location path for Aurora Characterization data in csv format
- `--feedback_path`: File location path for Aurora Feedback data in csv format
- `--monitoreo_path`: File location path for Aurora Monitoring data in csv format
- `--destination`: Carto data warehouse endpoint
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.

### How to load data to Carto Data Warehouse

The data processing scripts have two ways to handle the processed data, either export it as file (`csv` or `json`) and manually upload the file to Carto or upload direct to Carto Platform.

1. **Manual Upload**

You need to run a script adding the option `--output` which gives the output location for procesed data. By default the format will be in csv but you can also get json format by adding the `--format` option:

- **Example:**

```Bash
python process_service.py ~/raw_data.csv --output ~/output --format json
```

once you have the file ready, you can follow these steps to upload to carto data warehouse: [link](https://docs.carto.com/carto-user-manual/data-explorer/importing-data#how-to-import-data)

2. **Carto Data Client Upload**

You can also upload the data directly to carto data warehouse. You can use `--destination` flag to add your destination in carto using bigquery destination notation.

- **Example:**

```Bash
python process_service.py ~/raw_data.csv --destination qualified_table_name
```
