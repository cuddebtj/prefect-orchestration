create or replace view public.view_league_rosters_team as
with
team_roster_base as (
  select
    week,
    null::int as taxi,
    array_remove(array_agg(starter_key), null) as starters,
    -- settings,
    roster_id,
    null::int as reserve,
    array_remove(array_agg(player_key), null) as players,
    null::varchar as player_map,
    team_key as owner_id,
    -- metadata,
    ( split_part(team_key, '.', 1) || '.'
      || split_part(team_key, '.', 2) || '.'
      || split_part(team_key, '.', 3)
    ) as league_id,
    array[null::varchar] as keepers,
    null::varchar as co_owners
  from (
    select *
    from (
      select
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
    ) rost
    where rn < (
      case
        when selected_position in ('QB', 'TE', 'IR', 'DEF') then 2
        when selected_position in ('RB', 'WR', 'W/R/T') then 3
        when selected_position = 'BN' then 7 end
      )
  ) rost_base
  group by
    week,
    team_key,
    roster_id
),

team_info as (
  select distinct on (week::int, team_key)
    team_key,
    week,
    number_of_moves as total_moves,
    100 - faab_balance::decimal(3,0) as waiver_budget_used,
    waiver_priority as waiver_position
  from yahoo_data.teams
  where left(team_key, 3) = '423'
    and week::int > 0
  order by week::int, team_key, inserted_timestamp desc
),

settings as (
  select
    team_key,
    week,
    json_build_object(
      'wins', wins,
      'losses', losses,
      'ties', ties,
      'waiver_position', waiver_position,
      'waiver_budget_used', waiver_budget_used,
      'total_moves', total_moves,
      'ppts', ppts,
      'ppts_decimal', ppts_decimal,
      'fpts', fpts,
      'fpts_decimal', fpts_decimal,
      'fpts_against', fpts_against,
      'fpts_against', fpts_against,
      'fpts_against_decimal', fpts_against_decimal
    ) as settings
  from (
    select
      week_ranks.team_key,
      week_ranks.week,
      week_ranks.running_total_wins as wins,
      week_ranks.running_total_losses as losses,
      0 as ties,
      team_info.waiver_position,
      team_info.waiver_budget_used,
      team_info.total_moves,
      left(
        (week_ranks.running_points_for * 100)::decimal(6, 0)::text,
        length((week_ranks.running_points_for * 100)::decimal(6, 0)::text)-2
      )::decimal(4, 0) as ppts,
      right(
        (week_ranks.running_points_for * 100)::decimal(6, 0)::text, 2
      )::decimal(4, 0) as ppts_decimal,
      left(
        (week_ranks.running_points_for * 100)::decimal(6, 0)::text,
        length((week_ranks.running_points_for * 100)::decimal(6, 0)::text)-2
      )::decimal(4, 0) as fpts,
      right(
        (week_ranks.running_points_for * 100)::decimal(6, 0)::text, 2
      )::decimal(4, 0) as fpts_decimal,
      left(
        (week_ranks.running_points_against * 100)::decimal(6, 0)::text,
        length((week_ranks.running_points_against * 100)::decimal(6, 0)::text)-2
      )::decimal(4, 0) as fpts_against,
      right(
        (week_ranks.running_points_against * 100)::decimal(6, 0)::text, 2
      )::decimal(4, 0) as fpts_against_decimal
    from yahoo_data.view_weekly_rankings week_ranks
    left join team_info
      on team_info.team_key = week_ranks.team_key
      and team_info.week::int = week_ranks.week::int
  ) set
),

metadata as (
  select
    team_key,
    week,
    json_build_object(
      'streak', streak,
      'record', record
    ) as metadata
  from public.view_league_rosters_metadata
)

select
  team_roster_base.week,
  team_roster_base.taxi,
  team_roster_base.starters,
  settings.settings,
  team_roster_base.roster_id,
  team_roster_base.reserve,
  team_roster_base.players,
  team_roster_base.player_map,
  team_roster_base.owner_id,
  metadata.metadata,
  team_roster_base.league_id,
  team_roster_base.keepers,
  team_roster_base.co_owners
from team_roster_base
join metadata
  on metadata.team_key = team_roster_base.owner_id
  and metadata.week = team_roster_base.week
join settings
  on settings.team_key = team_roster_base.owner_id
  and settings.week = team_roster_base.week
order by week, owner_id;

