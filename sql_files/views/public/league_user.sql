create or replace view public.view_league_user as
select distinct on (manager_1_id::int, team_key)
  team_key,
  manager_1_id::int as user_id,
  manager_1_name as username,
  null::jsonb as metadata,
  true as is_owner,
  false as is_bot,
  name as display_name,
  team_logo_url as avatar
from yahoo_data.teams
order by manager_1_id::int, team_key, inserted_timestamp desc;
