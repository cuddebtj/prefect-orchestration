create or replace view public.view_league_users as
with
league_users as (
  select
    ( split_part(team_key, '.', 1) || '.'
      || split_part(team_key, '.', 2) || '.'
      || split_part(team_key, '.', 3)
    ) as league_id,
    jsonb_build_object(
      'team_key', team_key,
      'user_id', user_id,
      'username', username,
      'metadata', metadata,
      'is_owner', is_owner,
      'is_bot', is_bot,
      'display_name', display_name,
      'avatar', avatar
    ) as league_user
  from public.view_league_user
)

select
  league_id,
  array_agg(league_user) as league_users
from league_users
group by league_id;
