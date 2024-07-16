{% set mappings = {
    'OLD_COLUMN1': 'NEW_COLUMN1',
    'OLD_COLUMN2': 'NEW_COLUMN2'
} %}

{% set columns = [
    {'column': 'OLD_COLUMN1'},
    {'column': 'OTHER_COLUMN'}
] %}

{% set rename_sql = [] %}
{% for column in columns %}
    {% set col_name = column.column %}
    {% if col_name in mappings %}
        {% set new_name = mappings[col_name] %}
        {% do rename_sql.append(col_name ~ ' AS ' ~ new_name) %}
    {% else %}
        {% do rename_sql.append(col_name) %}
    {% endif %}
{% endfor %}

{{ return(rename_sql | join(',\n')) }}