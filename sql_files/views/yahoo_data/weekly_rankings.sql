create or replace view yahoo_data.view_weekly_rankings as
with
team_names as (
  select distinct on (team_key)
    ( split_part(team_key, '.', 1) || '.'
      || split_part(team_key, '.', 2) || '.'
      || split_part(team_key, '.', 3)
    ) as league_key,
    team_key,
    name
  from yahoo_data.teams
  order by team_key, inserted_timestamp desc
),

team_1 as (
  select distinct on (mch.week::decimal(2,0), mch.team_1_key)
    mch.league_key,
    mch.week::decimal(2,0) as week,
    mch.team_1_key as team_key,
    tn.name,
    mch.team_1_points::decimal(8, 2) as points_for,
    mch.team_2_points::decimal(8, 2) as points_against,
    case
      when coalesce(mch.winner_team_key, '') != '' then mch.winner_team_key
      when mch.team_1_points::decimal(8, 2) > mch.team_2_points::decimal(8, 2) then mch.team_1_key
      else mch.team_2_key end as winner_team_key
  from yahoo_data.matchups mch
  left join team_names tn
    on tn.team_key = mch.team_1_key
  order by mch.week::decimal(2,0), mch.team_1_key, inserted_timestamp desc
),

team_2 as (
  select distinct on (mch.week::decimal(2,0), mch.team_2_key)
    mch.league_key,
    mch.week::decimal(2,0) as week,
    mch.team_2_key as team_key,
    tn.name,
    mch.team_2_points::decimal(8, 2) as points_for,
    mch.team_1_points::decimal(8, 2) as points_against,
    case
      when coalesce(mch.winner_team_key, '') != '' then mch.winner_team_key
      when mch.team_2_points::decimal(8, 2) > mch.team_1_points::decimal(8, 2) then mch.team_2_key
      else mch.team_1_key end as winner_team_key
  from yahoo_data.matchups mch
  left join team_names tn
    on tn.team_key = mch.team_2_key
  order by mch.week::decimal(2,0), mch.team_2_key, inserted_timestamp desc
),

all_matchups as (
  select * from team_1
  union all
  select * from team_2
),

points_ranked as (
  select
    league_key,
    week,
    team_key,
    name,
    points_for,
    points_against,
    winner_team_key,
    rank() over (partition by week order by points_for desc) as points_rank
  from all_matchups
),

weekly_stats as (
  select
    league_key,
    week,
    team_key,
    name,
    points_for,
    points_against,
    winner_team_key,
    points_rank,
    case
      when team_key = winner_team_key and points_rank <= 5 then 2
      when team_key = winner_team_key and points_rank > 5 then 1
      when team_key != winner_team_key and points_rank <= 5 then 1
      when team_key != winner_team_key and points_rank > 5 then 0
      end as week_wins,
    case
      when team_key = winner_team_key and points_rank <= 5 then 0
      when team_key = winner_team_key and points_rank > 5 then 1
      when team_key != winner_team_key and points_rank <= 5 then 1
      when team_key != winner_team_key and points_rank > 5 then 2
      end as week_losses,
    sum(points_for) over (
      partition by team_key
      order by week
      rows between unbounded preceding and current row
    ) as running_points_for,
    sum(points_against) over (
      partition by team_key
      order by week
      rows between unbounded preceding and current row
    ) as running_points_against
  from points_ranked
),

running_tots as (
  select
    league_key,
    week,
    team_key,
    name,
    points_for,
    running_points_for,
    points_against,
    running_points_against,
    winner_team_key,
    points_rank,
    week_wins,
    week_losses,
    sum(week_wins) over (
      partition by team_key
      order by week
      rows between unbounded preceding and current row
    ) as running_total_wins,
    sum(week_losses) over (
      partition by team_key
      order by week
      rows between unbounded preceding and current row
    ) as running_total_losses
  from weekly_stats
),

overall_ranks as (
  select
    league_key,
    week,
    team_key,
    name,
    points_for,
    running_points_for,
    points_against,
    running_points_against,
    winner_team_key,
    points_rank,
    week_wins,
    week_losses,
    running_total_wins,
    running_total_losses,
    rank() over (partition by week order by running_total_wins desc) as wins_rank,
    rank() over (partition by week order by running_points_for desc) as total_pts_rank
  from running_tots
)

  select
    league_key,
    week,
    team_key,
    name,
    points_for,
    running_points_for,
    points_against,
    running_points_against,
    winner_team_key,
    points_rank,
    week_wins,
    week_losses,
    running_total_wins,
    running_total_losses,
    wins_rank,
    total_pts_rank,
    rank() over (partition by week order by wins_rank, total_pts_rank) as overall_rank
  from overall_ranks;
