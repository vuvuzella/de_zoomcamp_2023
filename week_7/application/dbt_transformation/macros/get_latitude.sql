{#
    This macro returns the latitude portion of the string "(latitude, longitude)"
#}

{% macro get_latitude(lat_long) -%}

    trim(split(replace(replace( {{ lat_long }}, '(', ''), ')', '' ), ',')[offset(0)])

{%- endmacro%}
