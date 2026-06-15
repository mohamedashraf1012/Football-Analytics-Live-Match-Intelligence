{% snapshot snap_dim_player %}

{{
    config(
        target_schema  = 'SNAPSHOTS',
        unique_key     = 'player_id',
        strategy       = 'check',
        check_cols     = ['current_club_id', 'current_club_name', 'is_active',
                          'contract_expiration_date', 'current_market_value_in_eur']
    )
}}

SELECT
    player_id,
    player_full_name,
    current_club_id,
    current_club_name,
    is_active,
    contract_expiration_date,
    current_market_value_in_eur,
    loaded_at
FROM {{ ref('stg_players') }}

{% endsnapshot %}