{% snapshot creditcard_fraud_history %}

{{
    config(
        target_database='dellstore',
        target_schema='public',
        unique_key='id',
        strategy='check',
        check_cols=['netamount']
    )
}}

SELECT * FROM {{ref('creditcard_fraud_agg')}}

{% endsnapshot %}