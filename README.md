# mapper-dnb

## Overview

The [dnb_mapper.py](dnb_mapper.py) python script converts Dun & Bradstreet (DNB) files to json files ready to load into Senzing.  This includes the following formats ...

- Companies and their principles **(CMPCVF)** json format
- Global contacts **(GCA)** tab delimited csv format
- Ultimate beneficial owners **(UBO)** tab delimited csv format

Normally these are provided by DNB on request and placed on an FTP server for you to download.

*Warning: the [dnb_formats.json](dnb_formats.json) file contains the exact structure of these files.   You may need to send these formats to DNB so they know exactly how to create them!*

Loading DNB data into Senzing requires additional features and configurations. These are contained in the
[dnb_config_updates.g2c](dnb_config_updates.g2c) file.

Usage:

```console
python3 dnb_mapper.py --help
usage: dnb_mapper.py [-h] [-f DNB_FORMAT] [-i INPUT_SPEC] [-o OUTPUT_PATH]
                     [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -f DNB_FORMAT, --dnb_format DNB_FORMAT
                        choose CMPCVF, GCA, UBO or UBO_ALONE
  -i INPUT_SPEC, --input_spec INPUT_SPEC
                        the name of one or more DNB files to map (place in
                        quotes if you use wild cards)
  -o OUTPUT_PATH, --output_path OUTPUT_PATH
                        output directory or file name for mapped json records
  -l LOG_FILE, --log_file LOG_FILE
                        optional statistics filename (json format).
```

*note:* The format UBO_ALONE should only be used if you are *only* loading the UBO file and wish to 
also map the corporate hierarchy information it contains.  The most complete corporate hierarchy 
information is contained in the CMPCVF feed and should be gotten from there if possible.


## Contents

1. [Prerequisites](#prerequisites)
1. [Installation](#installation)
1. [Configuring Senzing](#configuring-senzing)
1. [Running the mapper](#running-the-mapper)
1. [Loading into Senzing](#loading-into-senzing)

### Prerequisites

- python 3.6 or higher
- Senzing API version 1.15 or higher

### Installation

Place the the following files on a directory of your choice ...

- [dnb_mapper.py](dnb_mapper.py)
- [dnb_config_updates.g2c](dnb_config_updates.g2c)
- [dnb_formats.json](dnb_formats.json)

### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

From the /opt/senzing/g2/python directory ...

```console
python3 G2ConfigTool.py <path-to-file>/dnb_config_updates.g2c
```

This will step you through the process of adding the data sources, entity types, features, attributes and other settings needed to load this watch list data into Senzing. After each command you will see a status message saying "success" or "already exists".  For instance, if you run the script twice, the second time through they will all say "already exists" which is OK.

### Running the mapper

First, download the DNB files you want to load from the DNB FTP site.  Since the data files are so large, these are normally split into multiple files.

Second, run the mapper. Example usage:

```console
python3 dnb_mapper.py -f CMPCVF -i "./input/CMPCVF*.txt" -o ./output -l cmpcvf_stats.json

python3 dnb_mapper.py -f GCA -i "./input/GCA*.txt" -o ./output -l gca_stats.json

python3 dnb_mapper.py -f UBO -i "./input/UBO*.txt" -o ./output -l ubo_stats.json
    or
python3 dnb_mapper.py -f UBO_ALONE -i "./input/UBO*.txt" -o ./output -l ubo_stats.json
```

The output file defaults to the same name and location as the input file and a .json extension is added.

*It is critical that the -f file format match the input files exactly!*

*Note: Normally the company hierarchy comes from the CMPCVF format.  However, the UBO format also contains a trimmed down version of company records and their hierarchy.  Execute the 4th command above to capture the company hierarchy from the UBO file rather than the CMPCVF file.*

### Loading into Senzing

If you use the G2Loader program to load your data, its best to list the mapped json files you want to load in a project file.  There is an example of one in your senzing instalation here: /opt/senzing/g2/python/demo/sample/project.csv.  Then from from the /opt/senzing/g2/python directory ...

```console
python3 G2Loader.py -p <name of project file>
```

If you use the API directly, then you just need to perform an addRecord() for each line of each mapped file.
