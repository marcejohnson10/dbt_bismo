{% materialization materialized_table, adapter='snowflake' -%}
  {% set original_query_tag = set_query_tag() %}
  {% set full_refresh_mode = (should_full_refresh()) %}
  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  
  {#
    -- horrid hack for now, until we can make actual changes in dbt-snowflake plugin
    -- to support 'materialized_table' as a relation type
    -- for now, just don't ask
  #}
  {% set is_materialized_table = (existing_relation.type == 'external') %}
  {{ run_hooks(pre_hooks) }}
  {% if (existing_relation is none or full_refresh_mode) %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif not is_materialized_table %}
      {#-- Can't overwrite a view with a table - we must drop --#}
      {{ log("Dropping relation " ~ target_relation ~ " because it is a " ~ existing_relation.type ~ " and this model is a materialized view.") }}
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% else %}
      {#-- eventually, we expect this to be a no-op --#}
      {% set build_sql = get_refresh_materialized_table_sql(existing_relation) %}
  {% endif %}
  {% if build_sql %}
      {% call statement("main") %}
          {{ build_sql }}
      {% endcall %}
  {% else %}
    {{ store_result('main', 'NO CHANGE') }}
  {% endif %}
  {{ run_hooks(post_hooks) }}
  {% do persist_docs(target_relation, model) %}
  {% do unset_query_tag(original_query_tag) %}
  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
{% macro get_create_table_as_sql(temporary, relation, sql) -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- set auto_refresh = config.get('auto_refresh', False) or config.get('materialized') == 'materialized_table' -%}
  {%- set lag = config.get('lag') -%}
  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
    {%- set cluster_by_string = none -%}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
      create or replace {% if temporary -%}
        temporary
      {%- elif auto_refresh -%}
        materialized
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }}
      {% if auto_refresh and lag %} lag = '{{ lag }}' {% endif %}
      {% if copy_grants and not temporary -%} copy grants {%- endif %} as
      (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ sql }}
        {%- endif %}
      );
    {% if cluster_by_string is not none and not temporary -%}
      alter table {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter table {{relation}} resume recluster;
    {%- endif -%}
{% endmacro %}
{% macro get_refresh_materialized_table_sql(relation) %}
  {% set dml %}
    alter materialized table {{ relation }} refresh
  {% endset %}
  
  {{ return(snowflake_dml_explicit_transaction(dml)) }}
{% endmacro %}