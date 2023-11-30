# Files connected to Carto

1. Services data:

   - Output file: output.csv is used in "Inicio" and "Servicios" Carto pages of second round.
   - This file is created using: services.ipynb
   - services.ipynb calls the following json files:
     - codification\*dict.json
     - rename_columns.json
   - Raw data :'Caracterización*de_punto_de_servicio\*-\_Aurora_Office*-_Ronda_2_-_all_versions_-_False_-\_2023-11-28-13-59-47.csv' file from Kobo.

2. First connection data:

   - Output file: aurora_round_2.csv is used in "Inicio" and "Flujos Migratorios" Carto pages of second round.
   - This file is created using: Aurora.ipynb
   - Aurora.ipynb calls the following json files:
     countries_dict.json
     country_column_dict.json
   - Raw data: Aurora v2.1 data file - caracterización.csv
     Aurora v2.1 data file - ayudaHumanitaria.csv
     These files are downloaded from GoogleSheet (Aurora v2.1 data file)

3. Geographical coordinates of first conecctions and monitorings data:
   - Output file: Carto_map.csv is used in "Conexiones en la ruta" Carto page of second round.
   - This file is created using: monitoreos.ipynb
   - Raw data: Aurora v2.1 data file - caracterización.csv
     - Aurora v2.1 data file - ayudaHumanitaria.csv
     - Aurora v2.1 data file - monitoreo.csv
     - These files are downloaded from GoogleSheet (Aurora v2.1 data file)
