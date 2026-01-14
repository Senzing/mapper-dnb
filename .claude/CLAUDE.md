# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data mapper that converts Dun & Bradstreet (DNB) files into JSON format for loading into Senzing entity resolution system. It supports three DNB data formats:

- **CMPCVF**: Companies, executives, and corporate hierarchy (JSON input)
- **GCA**: Global contacts (tab/pipe/CSV delimited)
- **UBO**: Ultimate beneficial owners (tab/pipe/CSV delimited)
- **UBO_ALONE**: Standalone UBO with embedded corporate hierarchy

## Development Commands

```bash
# Create virtual environment and install all dependencies
python -m venv ./venv
source ./venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --group all .

# Run linting (matches CI)
pylint $(git ls-files '*.py' ':!:docs/source/*')

# Run the mapper
python3 src/dnb_mapper.py -f CMPCVF -i "./input/CMPCVF*.txt" -o ./output -l stats.json
```

## Code Architecture

The mapper is a single-file Python script (`src/dnb_mapper.py`) with format-specific transformation functions:

- `format_CMPCVF()`: Processes company/executive JSON records, creates relationships to parent companies and extracts principles (executives)
- `format_GCA()`: Processes contact records with group associations to companies
- `format_UBO()`: Processes ownership records linking owners to subject companies
- `format_UBO2()` / `format_UBO_SUBJECT()`: Handle standalone UBO format with depth-based ownership hierarchies

**Key data flow**: Input files are read according to schema definitions in `src/dnb_formats.json`, transformed to Senzing JSON format with appropriate attributes (DUNS_NUMBER, names, addresses, relationships), and written to output files.

**Relationships**: The mapper creates relationship links using REL_ANCHOR_DOMAIN/KEY (what this record can be pointed to by) and REL_POINTER_DOMAIN/KEY/ROLE (what this record points to). GROUP_ASSN_ID links people to their associated companies.

## Configuration Files

- `src/dnb_formats.json`: Schema definitions with column mappings for each DNB format
- `src/dnb_config_updates.g2c`: Senzing configuration script for required data sources, features, and attributes
