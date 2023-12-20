create or replace view public.view_nfl_state as
select
  start_date::text as season_start_date,
  (season - 1)::text as previous_season,
  season::text as league_create_season,
  current_week as display_week,
  season::text as league_season,
  'regular' as season_type,
  season::text as season,
  current_week as leg,
  current_week as week
from yahoo_data.view_leagues
where season = (select max(season) from yahoo_data.view_leagues);
