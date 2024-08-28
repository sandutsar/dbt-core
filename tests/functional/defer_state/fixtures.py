seed_csv = """id,name
1,Alice
2,Bob
"""

table_model_sql = """
{{ config(materialized='table') }}
select * from {{ ref('ephemeral_model') }}

-- establish a macro dependency to trigger state:modified.macros
-- depends on: {{ my_macro() }}
"""

table_model_now_view_sql = """
{{ config(materialized='view') }}
select * from {{ ref('ephemeral_model') }}

-- establish a macro dependency to trigger state:modified.macros
-- depends on: {{ my_macro() }}
"""

table_model_now_incremental_sql = """
{{ config(materialized='incremental', on_schema_change='append_new_columns') }}
select * from {{ ref('ephemeral_model') }}

-- establish a macro dependency to trigger state:modified.macros
-- depends on: {{ my_macro() }}
"""

changed_table_model_sql = """
{{ config(materialized='table') }}
select 1 as fun
"""

view_model_sql = """
select * from {{ ref('seed') }}

-- establish a macro dependency that trips infinite recursion if not handled
-- depends on: {{ my_infinitely_recursive_macro() }}
"""

view_model_now_table_sql = """
{{ config(materialized='table') }}
select * from {{ ref('seed') }}

-- establish a macro dependency that trips infinite recursion if not handled
-- depends on: {{ my_infinitely_recursive_macro() }}
"""

changed_view_model_sql = """
select * from no.such.table
"""

ephemeral_model_sql = """
{{ config(materialized='ephemeral') }}
select * from {{ ref('view_model') }}
"""

changed_ephemeral_model_sql = """
{{ config(materialized='ephemeral') }}
select * from no.such.table
"""

schema_yml = """
version: 2
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
"""

no_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config: {}
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

disabled_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
      enabled: False
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

modified_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: user_name
        data_type: text
"""

unenforced_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: False
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

disabled_unenforced_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: False
      enabled: False
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

versioned_no_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config: {}
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

versioned_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

disabled_versioned_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
      enabled: False
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

versioned_modified_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: True
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: user_name
        data_type: text
"""

disabled_versioned_unenforced_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: False
      enabled: False
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

versioned_unenforced_contract_schema_yml = """
version: 2
models:
  - name: table_model
    config:
      contract:
        enforced: False
    versions:
      - v: 1
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

constraint_schema_yml = """
version: 2
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
  - name: table_model
    config:
      contract:
        enforced: True
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        constraints:
          - type: not_null
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

modified_column_constraint_schema_yml = """
version: 2
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
  - name: table_model
    config:
      contract:
        enforced: True
    constraints:
      - type: primary_key
        columns: [id]
    columns:
      - name: id
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

modified_model_constraint_schema_yml = """
version: 2
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
  - name: table_model
    config:
      contract:
        enforced: True
    columns:
      - name: id
        constraints:
          - type: not_null
        data_type: integer
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name
        data_type: text
"""

exposures_yml = """
version: 2
exposures:
  - name: my_exposure
    type: application
    depends_on:
      - ref('view_model')
    owner:
      email: test@example.com
"""

macros_sql = """
{% macro my_macro() %}
    {% do log('in a macro' ) %}
{% endmacro %}
"""

infinite_macros_sql = """
{# trigger infinite recursion if not handled #}

{% macro my_infinitely_recursive_macro() %}
  {{ return(adapter.dispatch('my_infinitely_recursive_macro')()) }}
{% endmacro %}

{% macro default__my_infinitely_recursive_macro() %}
    {% if unmet_condition %}
        {{ my_infinitely_recursive_macro() }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}
"""

snapshot_sql = """
{% snapshot my_cool_snapshot %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['id'],
        )
    }}
    select * from {{ ref('view_model') }}

{% endsnapshot %}
"""


semantic_model_schema_yml = """
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name

semantic_models:
  - name: my_sm
    model: ref('view_model')
"""

modified_semantic_model_schema_yml = """
models:
  - name: view_model
    columns:
      - name: id
        data_tests:
          - unique:
              severity: error
          - not_null
      - name: name

semantic_models:
  - name: my_sm
    model: ref('view_model')
    description: modified description
"""

model_1_sql = """
select * from {{ ref('seed') }}
"""

modified_model_1_sql = """
select * from  {{ ref('seed') }}
order by 1
"""

model_2_sql = """
select id from  {{ ref('model_1') }}
"""

modified_model_2_sql = """
select * from  {{ ref('model_1') }}
order by 1
"""


group_schema_yml = """
groups:
  - name: finance
    owner:
      email: finance@jaffleshop.com

models:
  - name: model_1
    config:
      group: finance
  - name: model_2
    config:
      group: finance
"""


group_modified_schema_yml = """
groups:
  - name: accounting
    owner:
      email: finance@jaffleshop.com
models:
  - name: model_1
    config:
      group: accounting
  - name: model_2
    config:
      group: accounting
"""

group_modified_fail_schema_yml = """
groups:
  - name: finance
    owner:
      email: finance@jaffleshop.com
models:
  - name: model_1
    config:
      group: accounting
  - name: model_2
    config:
      group: finance
"""

metricflow_time_spine_sql = """
SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day
"""
