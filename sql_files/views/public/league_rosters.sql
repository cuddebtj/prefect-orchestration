create or replace view public.view_league_rosters as
with
rost as (
  select
    jsonb_build_object(
      'week', week,
      'taxi', taxi,
      'starters', starters,
      'settings', settings,
      'roster_id', roster_id,
      'reserve', reserve,
      'players', players,
      'player_map', player_map,
      'owner_id', owner_id,
      'metadata', metadata,
      'league_id', league_id,
      'keepers', keepers,
      'co_owners', co_owners
    ) as league_rosters,
    league_id
  from public.view_league_rosters_team
  where week::int = (select week::int from public.view_nfl_state)
)

select
  league_id,
  array_agg(league_rosters) as league_rosters
from rost
group by league_id;
