create or replace view public.view_league_matchups_team_roster as
select
  rost.league_id,
  rost.week,
  rost.player_key,
  rost.team_key,
  rost.roster_id,
  rost.starter_key,
  ftsy_pts.total_points
from (
  select *
  from (
    select
      ( split_part(team_key, '.', 1) || '.'
        || split_part(team_key, '.', 2) || '.'
        || split_part(team_key, '.', 3)
      ) as league_id,
      row_number() over (partition by week::int, team_key, selected_position order by max(inserted_timestamp) desc) as rn,
      week::int as week,
      team_key,
      player_key,
      selected_position,
      split_part(team_key, '.', 5)::int as roster_id,
      case
        when selected_position in ('BN', 'IR') then null
        else player_key end as starter_key
    from yahoo_data.rosters
    group by
      week::int,
      team_key,
      player_key,
      selected_position,
      team_key
  ) rost_base
  where rn < (
  case
    when selected_position in ('QB', 'TE', 'IR', 'DEF') then 2
    when selected_position in ('RB', 'WR', 'W/R/T') then 3
    when selected_position = 'BN' then 7 end
  )
) rost
left join (
  select
    week,
    player_key,
    full_name,
    sum(fantasy_points) as total_points
  from yahoo_data.view_player_stats
  group by week, player_key, full_name
) ftsy_pts
  on ftsy_pts.player_key = rost.player_key
  and ftsy_pts.week = rost.week::int;
