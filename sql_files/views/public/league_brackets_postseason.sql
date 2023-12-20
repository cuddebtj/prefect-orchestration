create or replace view public.view_league_postseason_round_one as
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
    and week::int = (select playoff_week_start::int from curr_season)
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
    select team_1_key, team_1_points
    from yahoo_matchups
    where week::int = (select playoff_week_start::int from curr_season)
    union all
    select team_2_key, team_2_points
    from yahoo_matchups
    where week::int = (select playoff_week_start::int from curr_season)
  ) team_pts
    on rks.team_key = team_pts.team_1_key
  where rks.week::int = (select playoff_week_start::int - 1 from curr_season)
    and rks.league_key = (select league_id from curr_season)
),

round_one as (
  select
    leag_mtch.matchup_id,
    team1.league_key,
    team1.week,
    team1.playoff_id,
    team1.team_1_key,
    1 as round,
    team1.team_1_points::decimal(8, 2) as team_1_points,
    coalesce(team2.team_1_key, '423.l.bye_week.t.0') as team_2_key,
    team2.team_1_points::decimal(8, 2) as team_2_points,
    case
      when team1.team_1_points > team2.team_1_points then team1.team_1_key
      when team1.team_1_points < team2.team_1_points then team2.team_1_key
      end as winner_team_key,
    case
      when team1.team_1_points > team2.team_1_points then team2.team_1_key
      when team1.team_1_points < team2.team_1_points then team2.team_1_key
      end as loser_team_key,
    case
      when team1.playoff_id > 4 then 'losers'
      when team1.playoff_id < 3 then 'winners'
      else 'consolation' end as bracket_type
  from playoff_seeds team1
  left join (
    select * from playoff_seeds where id_filter = 2
  ) team2
    on team2.playoff_id = team1.playoff_id
  left join (
    select
      matchup_id,
      league_key,
      week,
      roster_id
    from public.view_league_matchups_team
  ) leag_mtch
    on split_part(team1.team_1_key, '.', 5)::int = leag_mtch.roster_id
    and team1.week = leag_mtch.week
    and team1.league_key = leag_mtch.league_key
  where team1.id_filter = 1
)

select
  league_key,
  bracket_type,
  playoff_id,
  split_part(team_1_key, '.', 5)::int as t1,
  split_part(team_2_key, '.', 5)::int as t2,
  case when winner_team_key is null then null else split_part(winner_team_key, '.', 5)::int end as w,
  case when loser_team_key is null then null else split_part(loser_team_key, '.', 5)::int end as l,
  round as r,
  matchup_id as m
from round_one;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_postseason_round_two as
with
round_one as (
  select
    league_key,
    bracket_type,
    t1::int as t1,
    t2::int as t2,
    w,
    l,
    r,
    m
  from public.view_league_postseason_round_one
),

generate_round_two as (
  select
    (select max(m) from round_one) + row_number() over (order by null) as m
  from generate_series((select max(m) from round_one), (select max(m) + 4 from round_one)) i
),

round_two as (
  select
    (select distinct league_key from round_one) as league_key,
    case
      when m < (select max(m) + 3 from round_one) then 'winners'
      when m = (select max(m) + 3 from round_one) then 'consolation'
      when m > (select max(m) + 3 from round_one) then 'losers'
      end as bracket_type,
    case
      when m = (select max(m) + 1 from round_one) then (select w from round_one where m = (select min(m) from round_one))
      when m = (select max(m) + 2 from round_one) then (select l from round_one where m = (select min(m) from round_one))
      when m = (select max(m) + 3 from round_one) then (select t1 from round_one where m = (select min(m) + 2 from round_one))
      when m = (select max(m) + 4 from round_one) then (select w from round_one where m = (select min(m) + 4 from round_one))
      when m = (select max(m) + 5 from round_one) then (select l from round_one where m = (select min(m) + 4 from round_one))
      end as t1,
    case
      when m = (select max(m) + 1 from round_one) then 'w.' || (select min(m) from round_one)::text
      when m = (select max(m) + 2 from round_one) then 'l.' || (select min(m) from round_one)::text
      when m = (select max(m) + 3 from round_one) then 'b.' || (select min(m) + 2 from round_one)::text
      when m = (select max(m) + 4 from round_one) then 'w.' || (select min(m) + 4 from round_one)::text
      when m = (select max(m) + 5 from round_one) then 'l.' || (select min(m) + 4 from round_one)::text
      end as t1_from,
    case
      when m = (select max(m) + 1 from round_one) then (select w from round_one where m = (select min(m) + 1 from round_one))
      when m = (select max(m) + 2 from round_one) then (select l from round_one where m = (select min(m) + 1 from round_one))
      when m = (select max(m) + 3 from round_one) then (select t1 from round_one where m = (select min(m) + 3 from round_one))
      when m = (select max(m) + 4 from round_one) then (select w from round_one where m = (select min(m) + 5 from round_one))
      when m = (select max(m) + 5 from round_one) then (select l from round_one where m = (select min(m) + 5 from round_one))
      end as t2,
    case
      when m = (select max(m) + 1 from round_one) then 'w.' ||  (select min(m) + 1 from round_one)::text
      when m = (select max(m) + 2 from round_one) then 'l.' ||  (select min(m) + 1 from round_one)::text
      when m = (select max(m) + 3 from round_one) then 'b.' ||  (select min(m) + 3 from round_one)::text
      when m = (select max(m) + 4 from round_one) then 'w.' ||  (select min(m) + 5 from round_one)::text
      when m = (select max(m) + 5 from round_one) then 'l.' ||  (select min(m) + 5 from round_one)::text
      end as t2_from,
    null::int as w,
    null::int as l,
    2 as r,
    m,
    case
      when m = (select max(m) + 1 from round_one) then 1
      when m = (select max(m) + 2 from round_one) then 3
      when m = (select max(m) + 3 from round_one) then 5
      when m = (select max(m) + 4 from round_one) then 7
      when m = (select max(m) + 5 from round_one) then 9
      end as p
  from generate_round_two
)

select
  league_key,
  bracket_type,
  t1,
  jsonb_build_object(
    split_part(t1_from, '.', 1), split_part(t1_from, '.', 2)::int
  ) as t1_from,
  t2,
  jsonb_build_object(
    split_part(t2_from, '.', 1), split_part(t2_from, '.', 2)::int
  ) as t2_from,
  w,
  l,
  r,
  m,
  p
from round_two;
