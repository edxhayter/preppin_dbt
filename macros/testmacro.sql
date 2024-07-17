-- Macro to dynamically rename a sql table with a rename reference table.
-- The structure of the rename table should be two columns with a row for each column to be renamed. One column for the original name and one column for the new name.

{% macro test_rename(rename_table, source_relation) %}

    {% if execute %}

    {# Log the parameters to check their values #}
    {{ log("rename_table: " ~ rename_table, info=True) }}
    {{ log("source_relation: " ~ source_relation, info=True) }}

{# Section 1 get a dictionary of renames from the model that produces the rename table #}

    {# call the table with the rename values #}
    {% set rename_table_call = "select * from " ~ ref(rename_table) %}
    {{ log("rename_table_call: " ~ rename_table_call, info=True) }}

    {# run the query and then populate a list of mappings #}
    {% set results = run_query(rename_table_call) %}
    {% if results %}
        {% for row in results %}
            {{ log("row: " ~ row, info=True) }}
        {% endfor %}
    {% else %}
        {{ log("No results returned from query", info=True) }}
    {% endif %}

    {% else %}

    {# dummy sql for parsing phase #}
    select 1 

    {% endif %}

{% endmacro %}
