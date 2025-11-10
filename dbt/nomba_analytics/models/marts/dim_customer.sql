with users as (
  select *,
    row_number() over (partition by source_id order by updated_at desc nulls last) rn
  from {{ ref('stg_mongo_user') }}
)
select source_id, first_name, last_name, occupation, state, updated_at
from users
where rn = 1
