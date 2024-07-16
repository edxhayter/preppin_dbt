-- Macro to dynamically rename a sql table with a rename reference table.
-- The structure of the rename table should be two columns with a row for each column to be renamed. One column for the original name and one column for the new name.

{% macro dynamic_rename(rename_table, source_relation) %}

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
    {{ log("results: " ~ results, info=True) }}
    
    {# empty mappings dictionary to populate with a for loop #}
    {# Ensure results are not empty #}
    {% if results is not none and results.rows|length > 0 %}
        {% set mappings = {} %}
        {# Log the results.rows structure #}
        {{ log("Results rows structure: " ~ results.rows, info=True) }}

        {# for loop to populate mappings #}
        {% for row in results.rows %}
            {% set old_name = row[1] %}
            {% set new_name = row[0] %}
            {{ log("Processing row - Old Header: " ~ old_header ~ ", New Header: " ~ new_header, info=True) }}
            {% do mappings.update({header: new_header}) %}
        {% endfor %}
        {{ log("Updated mappings: " ~ mappings, info=True) }}

    {% else %}
        {# If no results, return an empty string to avoid further errors #}
        {{ exceptions.raise_compiler_error("Error: No values returned from the rename table query") }}
    {% endif %}

    {# Log the mappings dictionary #}
    {{ log("mappings: " ~ mappings, info=True) }}


{# Section 2: Get the column names for the specified table using the adapter object (this avoids a macro dependency on dbt utils by building the functionality into the macro) #}

    {# get the columns from the table to be renamed #} 
    {{ log("source_ref: " ~ source_ref, info=True) }}

    {% set columns = adapter.get_columns_in_relation( ref(source_relation) ) %}

    {# empty dictionary to hold the renaming sql #}
    {% set rename_sql = [] %}

    {# apply the renaming in a for loop #}
    {% for column in columns %}
        {% set col_name = column.column %}

        {# conditional is this a column that has renaming rules #}
        {% if col_name in mappings %}
            {% set new_name = mappings[col_name] %}

            {% do rename_sql.append(col_name ~ ' as ' ~ new_name) %}

        {% else %}

            {% do rename_sql.append(col_name) %}

        {% endif %}

    {% endfor %}

{{ return (rename_sql | join(',\n')) }}

{% else %}

{# dummy sql for parsing phase #}
select 1 

{% endif %}

{% endmacro %}




