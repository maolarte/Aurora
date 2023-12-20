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
$ python process_monitorepss.py  {path/to/file} --output_path path/to/file --format csv
```

The options available:

- `{path/to/file}`: File path, the location of which the service data file is located. Also accepts https file endpoints.
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.
  An example to run it is:

```Bash
cd path/to/repo
$ cp source/file target/path
$ pip install...
$ python process_services.py path/to/file
```

### `process_aurora`

This script process data collected using Landbot. It uses three data sheets

- ayudaHumanitaria
- caracterización

The steps are:

1. Make sure the required packages are install
2. Run the script in your terminal.

- **Example:**

```Bash
$ python process_monitorepss.py --cara_path path/to/file --feedback_path path/to/file --output_path path/to/file --format csv
```

The option available:

- `--cara_path`: File location path for Aurora Characterization data
- `--feedback_path`: File location path for Aurora Feedback data
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.

## `process_monitoreps`

This script process data collected using Landbot. It uses three data sheets

- ayudaHumanitaria
- caracterización
- monitoreo

The steps are:

1. Make sure the required packages are install
2. Run the script in your terminal. The following option are available:

```Bash
$ python process_monitorepss.py --cara_path path/to/file --feedback_path path/to/file --monitero_path path/to/file --output_path path/to/file --format csv
```

The option available:

- `--cara_path`: File location path for Aurora Characterization data
- `--feedback_path`: File location path for Aurora Feedback data
- `--monitoreo_path`: File location path for Aurora Monitoreo data
- `--output`: Output name or output path
- `--format`: Output format if output path is given. Can either be `csv` or `json`.
