## notes

1. `dbt run --select +<model.sql> ...` will run the model and all its dependencies in DAG

- `--var 'var1: value'` - to set variables in run time

2. Basic Data Warehousing 101:

- `core` folder contains dimensions and facts. Materialize them as tables
- `staging` folder contains views. Need to define source of the views, defined in `schema.yml`

3. Template sql file susing jinja templating

- `{{ <an expression that resolves to a string> }}`
- `{% <a macro call or an expression that involves control flow> %}`
- `{# This is a Jinja comment #}`

4. Common jinja function:

- `source()` - references the source name defined in schema.yml
- `ref()` - to reference existing tables/views
- `config()` - sets table config/materialization strategy

5. Macros

- Can be defined in a separate sql file
- `{% macro my_macro(param1, param2) %}.... {% endmacro %}`

6. vars defined in `dbt_project.yml` are global variables

7. Tests are defined as select statements. If query result are not null, then tests have failed.

8. Documentation

- Columns
- Models
