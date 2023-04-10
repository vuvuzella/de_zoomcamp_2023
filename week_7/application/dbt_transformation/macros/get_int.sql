{#
    This macro cleans a number of form "x.0", to only return "x"
#}

{% macro get_int(input) -%}

    trim(split({{ input }}, '.')[offset(0)])

{%- endmacro%}
