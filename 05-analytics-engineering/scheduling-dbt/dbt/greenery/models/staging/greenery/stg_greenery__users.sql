with source as (

    select * from {{ source('greenery', 'users') }}

),

renamed_recasted as (

    select
        user_id as user_guid
        , first_name
        , last_name
        , email
        , phone_number
        , created_at as users_created_at_utc
        , updated_at as updated_at_utc
        , address_id as address_guid

    from source

)

select * from renamed_recasted