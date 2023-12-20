create or replace view public.view_draft_picks_player_metadata as
with
player_pct as (
  select distinct on (player_key)
    '' as years_exp,
    editorial_team_full_name as team,
    '' as status,
    'nfl' as sport,
    player_key as player_id,
    uniform_number as number,
    player_notes_last_timestamp as news_updated,
    last_name as last_name,
    injury_note as injury_status,
    first_name as first_name
  from yahoo_data.player_pct_owned
  order by player_key, inserted_timestamp desc
),

player_rosters as (
  select distinct on (player_key)
    editorial_team_full_name,
    injury_note,
    player_key,
    player_notes_last_timestamp,
    uniform_number as uniform_number
  from yahoo_data.rosters
  order by player_key, inserted_timestamp desc
),

player_data as (
  select distinct on (play.player_key)
    '' as years_exp,
    rost.editorial_team_full_name as team,
    '' as status,
    'nfl' as sport,
    play.player_key as player_id,
    rost.uniform_number as number,
    rost.player_notes_last_timestamp as news_updated,
    play.last_name as last_name,
    rost.injury_note as injury_status,
    play.first_name as first_name
  from yahoo_data.players play
  left join player_rosters rost
    on rost.player_key = play.player_key
  order by play.player_key, play.inserted_timestamp desc
),

all_player_data as (
  select
    years_exp,
    team,
    status,
    sport,
    player_id,
    number,
    news_updated,
    last_name,
    injury_status,
    first_name
  from player_pct
  union all
  select
    years_exp,
    team,
    status,
    sport,
    player_id,
    number,
    news_updated,
    last_name,
    injury_status,
    first_name
  from player_data
)

select distinct on (player_id)
  coalesce(years_exp, '') as years_exp,
  coalesce(team, '') as team,
  coalesce(status, '') as status,
  coalesce(sport, '') as sport,
  coalesce(player_id, '') as player_id,
  coalesce(number, '') as number,
  coalesce(news_updated, '') as news_updated,
  coalesce(last_name, '') as last_name,
  coalesce(injury_status, '') as injury_status,
  coalesce(first_name, '') as first_name
from all_player_data
where first_name != ''
order by player_id, news_updated desc;

