{% macro s3_to_redshift(s3_path, table_name) %}
COPY {{ table_name }}
FROM '{{ s3_path }}'
IAM_ROLE 'arn:aws:iam::your-account-id:role/RedshiftCopyRole'
FORMAT AS PARQUET;
{% endmacro %}

{% do run_query(s3_to_redshift('s3://your-bucket/path/to/parquet/', 'staging_table')) %}

SELECT * FROM staging_table;
