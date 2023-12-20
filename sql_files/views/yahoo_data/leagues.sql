-- drop view yahoo_data.view_leagues cascade;
create or replace view yahoo_data.view_leagues as
with
league as (
  select distinct on (league_key)
    league_key,
    game_code,
    draft_status,
    name,
    league_update_timestamp,
    num_teams::int as num_teams,
    season::int as season,
    replace(renew, '_', '.l.') as renew,
    replace(renewed, '_', '.l.') as renewed,
    start_week::int as start_week,
    start_date::date as start_date,
    end_week::int as end_week,
    end_date::date as end_date,
    current_week::int as current_week,
    logo_url,
    is_finished
  from yahoo_data.leagues
  order by league_key, inserted_timestamp desc
),

settings as (
  select distinct on (league_key)
    league_key,
    league_key || '.draft' as draft_id,
    draft_time::int as draft_time,
    draft_pick_time::int as draft_pick_time,
    'snake' as draft_type,
    num_playoff_teams::int as num_playoff_teams,
    num_playoff_consolation_teams::int as num_playoff_consolation_teams,
    playoff_start_week::int as playoff_start_week
  from yahoo_data.settings
  order by league_key, inserted_timestamp desc
)

select
  league.league_key,
  league.game_code,
  league.draft_status,
  league.name,
  league.league_update_timestamp,
  league.num_teams,
  league.season,
  league.renew,
  league.renewed,
  league.start_week,
  league.start_date,
  league.end_week,
  league.end_date,
  league.current_week,
  league.logo_url,
  settings.draft_id,
  settings.draft_time,
  settings.draft_pick_time,
  settings.draft_type,
  settings.num_playoff_teams,
  settings.num_playoff_consolation_teams,
  settings.playoff_start_week,
  league.is_finished
from league
join settings
  on settings.league_key = league.league_key;
