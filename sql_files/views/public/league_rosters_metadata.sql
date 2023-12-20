create or replace view public.view_league_rosters_metadata as
with
lags as (
  select
    team_key,
    week,
    h2h,
    is_winner,
    lag(is_winner)
      over (
        partition by team_key
        order by week
      ) as prev_is_winner
  from (
    select
      team_key as team_key,
      week::int as week,
      case when team_key = winner_team_key then 'W' else 'L' end h2h,
      team_key = winner_team_key as is_winner
    from yahoo_data.view_weekly_rankings
    where week::int <= (select max(week::int) from yahoo_data.view_weekly_rankings where coalesce(winner_team_key, '') != '')
  ) yd
),

all_weeks as (
  select
    *,
    row_number()
        over (
          partition by team_key, streak_group
          order by week asc, streak_group asc
        ) as expected_unsigned
  from (
    select
      *,
      sum(
        case
          when prev_is_winner <> is_winner or prev_is_winner is null then 1
          else 0 end)
        over (
          partition by team_key
          order by week, is_winner desc
        ) as streak_group
    from lags
  ) grps
),

curr_streak as (
  select
    team_key,
    week,
    case
      when is_winner then expected_unsigned
      else expected_unsigned * -1 end as streak
  from all_weeks
),

curr_record as (
  select
    team_key,
    week,
    string_agg(h2h, '')
      over (
        partition by team_key
        order by week
        rows between unbounded preceding and current row
      ) as record
  from all_weeks
)

select
  curr_record.team_key,
  curr_record.week,
  curr_record.record,
  case
    when curr_streak.streak < 0 then (-1 * curr_streak.streak)::text || 'L'
    else curr_streak.streak::text || 'W' end as streak
from curr_record
join curr_streak
  on curr_streak.team_key = curr_record.team_key
  and curr_streak.week = curr_record.week;
