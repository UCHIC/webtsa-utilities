# webtsa-utilities #

Back\-end utilities for the Web Time Series Analyst

###### Requirements ######

These tools are written for Python 2.7.X

To install the required packages to run these utilities, run the following command from the project's root directory:

```sh
pip install -r ./src/requirements.txt
```

You will need to populate a `settings.json` file in the `./src/` directory. You can create a copy of the settings template `settings_template.json` and rename it to `settings.json`. 

***

#### InfluxDB Updater Utility ####

###### Running the InfluxDB Updater #####

1. To run the InfluxDB Updater, ensure your `settings.json` file is correct.

2. Simply run the following command (arguments listed below are optional):
```sh
python ./src/InfluxUpdater.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_{script}_File.txt`; stderr output is not redirected to log file|

###### Determining the identifier for a given time series  ######

> (in this example, Site: PUPP2S; Var: Campbell_OBS-3+_Turb, QC: 1, Source: 67, Method: 2):

| Formatting Steps | Example |
| --- | --- |
|`wof_{site_code}_{var_code}_{qc_id}_{source_id}_{method_id}`|wof_PUPP2S_Campbell_OBS_3+_Turb_Raw_1_67_2 |
|Encode as a URI to sanitize chars while keeping uniqueness|wof_PUPP2S_Campbell_OBS_3%2B_Turb_1_67_2|
|Replace all non-word characters with an underscore|wof_PUPP2S_Campbell_OBS_3_2B_Turb_1_67_2|

###### Example python code: ######

```python 
def GetIdentifier(site_code, var_code, qc_id, source_id, method_id):
    pre_identifier = 'wof_{}_{}_{}_{}_{}'.format(site_code, var_code, qc_id, source_id, method_id)
    return re.sub('[\W]', '_', urllib.quote(pre_identifier, safe=''))
```

***

#### TSA Catalog Updater Utility #####


###### Running the TSA Catalog Updater #####

1. To run the TSA Catalog Updater, ensure your `settings.json` file is correct.

2. Simply run the following command (arguments listed below are optional):
```sh
python ./src/tsa_catalog_update.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_{script}_File.txt`; stderr output is not redirected to log file|
