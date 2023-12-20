create or replace view public.view_league_matchups_team as
with
curr_season as (
  select distinct
    league_id,
    settings::jsonb->>'playoff_week_start' as playoff_week_start
  from public.view_league
  where season = (select distinct season::int from public.view_nfl_state)
),

yahoo_matchups as (
  select
    ( split_part(team_1_key, '.', 1) || '.'
      || split_part(team_1_key, '.', 2) || '.'
      || split_part(team_1_key, '.', 3)
    ) as league_key,
    week::int as week,
    team_1_key,
    team_1_points::decimal(8, 2) as team_1_points,
    team_2_key,
    team_2_points::decimal(8, 2) as team_2_points,
    winner_team_key
  from yahoo_data.view_matchups
  where ( split_part(team_1_key, '.', 1) || '.'
      || split_part(team_1_key, '.', 2) || '.'
      || split_part(team_1_key, '.', 3)
    ) = (select league_id from curr_season)
),

playoff_seeds as (
  select
    rks.league_key,
    (select playoff_week_start::int from curr_season) as week,
    rks.team_key as team_1_key,
    team_pts.team_1_points as team_1_points,
    case
      when rks.overall_rank = 1 then 1
      when rks.overall_rank = 2 then 2
      when rks.overall_rank = 3 then 2
      when rks.overall_rank = 4 then 1
      when rks.overall_rank = 5 then 3
      when rks.overall_rank = 6 then 4
      when rks.overall_rank = 7 then 5
      when rks.overall_rank = 8 then 6
      when rks.overall_rank = 9 then 6
      when rks.overall_rank = 10 then 5
      end as playoff_id,
    row_number() over (
      partition by
        case
          when rks.overall_rank = 1 then 1
          when rks.overall_rank = 2 then 2
          when rks.overall_rank = 3 then 2
          when rks.overall_rank = 4 then 1
          when rks.overall_rank = 5 then 3
          when rks.overall_rank = 6 then 4
          when rks.overall_rank = 7 then 5
          when rks.overall_rank = 8 then 6
          when rks.overall_rank = 9 then 6
          when rks.overall_rank = 10 then 5
          end
      order by rks.overall_rank) as id_filter
  from yahoo_data.view_weekly_rankings rks
  left join (
    select team_1_key, team_1_points from yahoo_matchups where week::int = 16
    union all
    select team_2_key, team_2_points from yahoo_matchups where week::int = 16
  ) team_pts
    on rks.team_key = team_pts.team_1_key
  where rks.week::int = (select playoff_week_start::int - 1 from curr_season)
    and rks.league_key = (select league_id from curr_season)
),

playoff_matchups as (
  select
    team1.league_key,
    team1.week,
    team1.playoff_id,
    team1.team_1_key,
    team1.team_1_points::decimal(8, 2) as team_1_points,
    coalesce(team2.team_1_key, '423.l.bye_week.t.0') as team_2_key,
    team2.team_1_points::decimal(8, 2) as team_2_points
  from playoff_seeds team1
  left join (
    select * from playoff_seeds where id_filter = 2) team2
    on team2.playoff_id = team1.playoff_id
  where team1.id_filter = 1
),

match_ids as (
  select
    row_number() over (partition by league_key order by week, coalesce(playoff_id, roster_1_id)) as matchup_id,
    *
  from (
    select
      league_key,
      week,
      team_1_key,
      split_part(team_1_key, '.', 5)::int as roster_1_id,
      team_1_points,
      team_2_key,
      split_part(team_2_key, '.', 5)::int as roster_2_id,
      team_2_points,
      null as playoff_id
    from yahoo_matchups
    where week < (select playoff_week_start::int from curr_season)
    union all
    select
      league_key,
      week,
      team_1_key,
      split_part(team_1_key, '.', 5)::int as roster_1_id,
      team_1_points,
      team_2_key,
      split_part(team_2_key, '.', 5)::int as roster_2_id,
      team_2_points,
      playoff_id
    from playoff_matchups
  ) mtchs
),

team_match as (
  select
    league_key,
    week,
    matchup_id,
    roster_1_id as roster_id,
    team_1_points as points
  from match_ids
  union all
  select
    league_key,
    week,
    matchup_id,
    roster_2_id as roster_id,
    team_2_points as points
  from match_ids
)

select
  team_match.league_key,
  team_match.week,
  team_match.matchup_id,
  team_match.roster_id,
  team_match.points,
  array[null::numeric] as custom_points,
  array_remove(array_agg(rost.player_key), null) as players,
  array_remove(array_agg(rost.starter_key), null) as starters,
  array_remove(array_agg(case when coalesce(rost.starter_key, '') != '' then rost.total_points else null end), null) as starter_points,
  jsonb_object_agg(
    coalesce(rost.player_key, ''), rost.total_points
  ) as player_points
from team_match
left join public.view_league_matchups_team_roster rost
  on team_match.week = rost.week
  and team_match.roster_id = rost.roster_id
  and team_match.league_key = rost.league_id
where team_match.roster_id != 0
group by
  team_match.league_key,
  team_match.week,
  team_match.matchup_id,
  team_match.roster_id,
  team_match.points
order by week desc, matchup_id, roster_id;
