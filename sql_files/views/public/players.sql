create or replace view public.view_players as
with
league_players as (
  select
    player_id,
    jsonb_build_object(
      'player_id', player_id,
      'team', team,
      'full_name', full_name,
      'position', position,
      'metadata', metadata,
      'number', number,
      'injury_notes', injury_notes,
      'search_first_name', search_first_name,
      'fantasy_positions', fantasy_positions,
      'last_name', last_name,
      'search_last_name', search_last_name,
      'sport', sport,
      'news_updated', news_updated,
      'first_name', first_name,
      'active', active,
      'yahoo_id', yahoo_id
    ) as league_player
  from public.view_player_data
)

select
  player_id,
  array_agg(league_player) as player_data
from league_players
group by player_id;
