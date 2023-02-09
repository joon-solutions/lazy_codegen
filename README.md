# Lazy-codegen

Heavily 'inspired' by dbt-generator. Ok, I cloned the entire thing. I only take credit for the macros. The rest is thanks to Tuan Chris. I'm sorry that I butchered your project. 

The reasons I'm not PR this to dbt-generator are: 
- It's a public repo
- It is relying on homebrew macro that's not available on dbt hubs. Also it's only available for Snowflake/Bigquery right now.
- My code is far too spaghetti.

For sources with 10+ models, this package will save you a lot of time by generating base models in bulk and transform them for common fields automatically - casting timestamp field to assigned timezone, change boolean column_name  , and group columns together by data type. 

There is also the describe function allowing you to perform basic exploratory on your source tables. However, every describe function will run over your entire table, so ensure to use them with a condition clause. 
 
This currently works for Snowflake and Bigquery, for dbt version 1.0.8 and up. 

## Why lazycodegen 

Lazy codegen nicely sorts all columns base on their data type + column name. Also does some basic cleaning + parsing automatically. 
![image](https://user-images.githubusercontent.com/119023371/217585631-2add82de-4330-40a0-ab7e-889972718993.png)


Old version 

![image](https://user-images.githubusercontent.com/119023371/217585691-1d0182c4-3f05-4ed7-bf0b-d54502b304a3.png)

Just save time in base layer building - instead of parsing manually, it's done for you. 


## Installation

To use this package, you need dbt installed with a profile configured. You will also need to copy the lazy_codgen folder from this repo to your macro folder.  


Install the package in the same environment with your dbt installation by running: 

```bash
pip install -e /your/directory
```

This package should be executed inside your dbt repo. 

Note: If you want to use the lint function, you'll need Sqlfluff installed. 

## Generate base models

To generate base models, use the `dbt-generator generate` command. This is a wrapper around the `base_table_gen` macro that will generate the base models. It will then process to generate every table at once. You can limit/specify the ammount of model generated through either the -m tags or through the --source-index option ( input the starting line of the model you want to generate)

```
Usage: dbt-generator generate [OPTIONS]

  Gennerate base models based on a .yml source

Options:      
  -s, --source-yml PATH             Source .yml file to be used
  -o, --output-path PATH            Path to write generated models
  -t, --timezone STRING             Timezone to convert detected timestamp columns too. Cannot convert columns that are not detected as timestamp. 
  -m, --model STRING                Model name. Genereate only specified models in inputted list. 
  -c, --custom_prefix STRING        Enter a Custom String Prefix for Model Filename
  --model-prefix BOOLEAN            optional prefix of source_name + "_" to the resulting modelname.sql to avoid model name collisions across sources 
  --source-index INTEGER            Index of the source to generate base models for. 
  -l, --linting  FLAG               Lint the generated table using sqlfluff. 
  -d, --describe FLAG               If enabled, will run the describe macro. Describe macro will generate a description table, as well as warn of    possible issues in the dataset (null/na check, distinctive columns, duplicate data within columns, etc. )
  -cr, --corr    FLAG               If enabled, will run the correlation macro. Generate a table of correlation between all number columns. 
  -dc, --describe-condition STRING  Query condition for desribe and correlation tables, to prevent excessive cost. Might create data skews. 
  --help                            Show    this message and exit.

```

### Example

```bash
dbt-generator generate -s ./models/source.yml -o ./models/staging/source_name/ - l -d
```

This will read in the `source.yml` file and generate the base models in the `staging/source_name` folder. If you have multiple sources defined in your `yml` file, use the `--source-index` flag to specify which source you want to start generating base models for.

## Base transformation

Tranformation are handled automatically using the base_table_gen macro. 

Custom transformation can be added by: 
- Adding a new custom list + column detection condition
- Adding new parsing command within the generation file 
- Detailed customization guide will be updated. Read the comment in the base_table_gen macro will do too

