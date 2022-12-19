{% macro vertica__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', default=None) -%}
  {%- set order_by = config.get('order_by', default=None) -%}
  {%- set segmented_by_string = config.get('segmented_by_string', default=None) -%}
  {%- set segmented_by_all_nodes = config.get('segmented_by_all_nodes', default=True) -%}
  {%- set no_segmentation = config.get('no_segmentation', default=False) -%}
  {%- set partition_by_string = config.get('partition_by_string', default=None) -%}
  {%- set partition_by_group_by_string = config.get('partition_by_group_by_string', default=None) -%}
  {%- set partition_by_active_count = config.get('partition_by_active_count', default=None) -%}
  {%- set ksafe = config.get('ksafe', default=None) -%}

  {{ sql_header if sql_header }}

  create {% if temporary: -%}local temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if temporary: -%}on commit preserve rows{%- endif %}
  as (
    {{ sql }}
  )
  {% if order_by -%}
    order by {{ order_by | join(',') }}
  {% endif %}
  {% if segmented_by_string -%}
    segmented by {{ segmented_by_string }} {% if segmented_by_all_nodes %} ALL NODES {% endif %}
  {% endif %}
  {% if no_segmentation %}UNSEGMENTED ALL NODES{% endif %}
    {% if ksafe -%}
      ksafe {{ ksafe }}
    {% endif %}
    {% if partition_by_string -%}
      partition by {{ partition_by_string }}
    {% if partition_by_string and partition_by_group_by_string -%}
      group by {{ partition_by_group_by_string }}
    {% endif %}
    {% if partition_by_string and partition_by_active_count -%}
      ACTIVEPARTITIONCOUNT {{ partition_by_active_count }}
    {% endif %}
  {% endif %}
  ;
{% endmacro %}
