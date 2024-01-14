# Default Values

This directory containts constant variables mainly inform of JSON. These JSON constants include:

- `aurora_column_name.json`: A dictionary for columns names that need changing from raw data to match existing schema in carto data warehouse. Usage [`process_aurora.py`](/process_aurora.py)

- `codification_dict.json`: A dictionary of objects configurations for processing columns that need codify for better reduce data transfer sizes. Usage [`process_services.py`](/process_services.py). The configuration object has the following properties:

  - `target_column`: Column name that needs cooresponding to codification configuration.
  - `output_column`: column name which would be result of the codification.
  - `values_dict`: Object that contains values that need to be codified.
  - `other_value`: Other value in case it cannot be codified.

- `column_capitalisation.json`: Columns of which their values need to be capitalised. Usage [`process_services.py`](/process_services.py)

- `countries_dict.json`: Object of country name items that need to changed to approprite language values. Usage [`process_aurora.py`](/process_aurora.py).

- `country_column_dict.json`: Object of columns to which to retrieve coordianate data. Usage [`process_aurora.py`](/process_aurora.py).

- `organisation_names.json`: Object of organisation name items that need to changed to approprite language values. Usage [`process_services.py`](/process_services.py).

- `places.json`: Object of organisation name items that need to changed to approprite language values. Usage [`process_services.py`](/process_services.py).

- `rename_columns.json`: A dictionary for columns names that need changing from raw data to match existing schema in carto data warehouse

- `test_user_ids.json`: Identification number for test users that need to be removed. Usage [`process_aurora.py`](/process_aurora.py), [`process_monitoreos.py`](/process_monitoreos.py)

- `user_ids_ch.json`: Identification number for Chile users that whose attributes have to be changed.Usage [`process_aurora.py`](/process_aurora.py),

- `user_ids_col.json`: Identification number for Colombia users that whose attributes have to be changed. Usage [`process_aurora.py`](/process_aurora.py).

- `value_replacements.json`: A dictionary of values that need to be changed. Usage [`process_services.py`](/process_services.py)
