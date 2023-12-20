create or replace view public.view_league_matchups as
with
league_matchup_team as (
  select
    league_key,
    week,
    jsonb_build_object(
      'matchup_id', matchup_id,
      'roster_id', roster_id,
      'points', points,
      'custom_points', custom_points,
      'players', players,
      'starters', starters,
      'starter_points', starter_points,
      'player_points', player_points
    ) as league_matchup
  from public.view_league_matchups_team
)

select
  league_key,
  week,
  array_agg(league_matchups) as league_matchup
from league_players
group by league_key, week;
