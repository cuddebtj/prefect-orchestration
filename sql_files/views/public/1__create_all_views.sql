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
--------------------------------------------------------------------------------------------------------------------------------
create or replace view yahoo_data.view_player_stats as
with
stat_mod as (
  select distinct on (stat_id)
    stat_id,
    value::decimal(8, 2) modifier
  from yahoo_data.stat_modifiers
  order by stat_id, inserted_timestamp desc
),

stat_cat as (
  select distinct on (stat_id)
    stat_cat.stat_id,
    stat_cat.display_name,
    stat_mod.modifier
  from yahoo_data.stat_categories stat_cat
  left join stat_mod
    on stat_mod.stat_id = stat_cat.stat_id
  order by stat_id, inserted_timestamp desc
),

player_map as (
  select distinct on (player_key)
    player_key,
    full_name
  from yahoo_data.players
  where coalesce(player_key, '') != ''
  order by player_key, inserted_timestamp desc
),

player_stats as (
  select distinct on (week, player_key, stat_id)
    week::decimal(2,0) week,
    player_key,
    stat_id,
    stat_value::decimal(8, 0) stat_value,
    total_points::decimal(8, 2) total_points
  from yahoo_data.player_stats
  order by week, player_key, stat_id, inserted_timestamp desc
),

final_stats as (
  select
    player_stats.week,
    player_stats.player_key,
    player_map.full_name,
    player_stats.stat_id,
    stat_cat.display_name,
    player_stats.stat_value,
    coalesce(stat_cat.modifier, 0.00) stat_modifier,
    (coalesce(stat_cat.modifier, 0.00) * player_stats.stat_value)::decimal(8, 2) fantasy_points,
    player_stats.total_points
  from player_stats
  left join stat_cat
    on stat_cat.stat_id = player_stats.stat_id
  left join player_map
    on player_map.player_key = player_stats.player_key
)

select *
from final_stats
order by week, full_name, stat_value desc;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view yahoo_data.view_matchups as
with
team_names as (
  select distinct on (team_key)
    team_key,
    name
  from yahoo_data.teams
  order by team_key, inserted_timestamp desc
)

select distinct on (mch.week::decimal(2,0), mch.team_1_key, mch.team_2_key)
  mch.week::decimal(2,0) week,
  mch.team_1_key,
  tn_1.name team_1_name,
  mch.team_1_points::decimal(8, 2) team_1_points,
  mch.team_2_key,
  tn_2.name team_2_name,
  mch.team_2_points::decimal(8, 2) team_2_points,
  mch.winner_team_key,
  case
    when mch.winner_team_key = mch.team_1_key then tn_1.name
    when mch.winner_team_key = mch.team_2_key then tn_2.name
    end winner_team_name
from yahoo_data.matchups mch
left join team_names tn_1
  on tn_1.team_key = mch.team_1_key
left join team_names tn_2
  on tn_2.team_key = mch.team_2_key
order by mch.week::decimal(2,0), mch.team_1_key, mch.team_2_key, inserted_timestamp desc;

--------------------------------------------------------------------------------------------------------------------------------
create or replace view yahoo_data.view_leagues as
with
league as (
  select distinct on (league_key)
    league_key,
    game_code,
    draft_status,
    name,
    league_update_timestamp,
    num_teams::int as num_teams,
    season::int as season,
    replace(renew, '_', '.l.') as renew,
    replace(renewed, '_', '.l.') as renewed,
    start_week::int as start_week,
    start_date::date as start_date,
    end_week::int as end_week,
    end_date::date as end_date,
    current_week::int as current_week,
    logo_url,
    is_finished
  from yahoo_data.leagues
  order by league_key, inserted_timestamp desc
),

settings as (
  select distinct on (league_key)
    league_key,
    league_key || '.draft' as draft_id,
    draft_time::int as draft_time,
    draft_pick_time::int as draft_pick_time,
    'snake' as draft_type,
    num_playoff_teams::int as num_playoff_teams,
    num_playoff_consolation_teams::int as num_playoff_consolation_teams,
    playoff_start_week::int as playoff_start_week
  from yahoo_data.settings
  order by league_key, inserted_timestamp desc
)

select
  league.league_key,
  league.game_code,
  league.draft_status,
  league.name,
  league.league_update_timestamp,
  league.num_teams,
  league.season,
  league.renew,
  league.renewed,
  league.start_week,
  league.start_date,
  league.end_week,
  league.end_date,
  league.current_week,
  league.logo_url,
  settings.draft_id,
  settings.draft_time,
  settings.draft_pick_time,
  settings.draft_type,
  settings.num_playoff_teams,
  settings.num_playoff_consolation_teams,
  settings.playoff_start_week,
  league.is_finished
from league
join settings
  on settings.league_key = league.league_key;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_nfl_state as
select
  start_date::text as season_start_date,
  (season - 1)::text as previous_season,
  season::text as league_create_season,
  current_week as display_week,
  season::text as league_season,
  'regular' as season_type,
  season::text as season,
  current_week as leg,
  current_week as week
from yahoo_data.view_leagues
where season = (select max(season) from yahoo_data.view_leagues);
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_user as
select distinct on (manager_1_id::int, team_key)
  team_key,
  manager_1_id::int as user_id,
  manager_1_name as username,
  null::jsonb as metadata,
  true as is_owner,
  false as is_bot,
  name as display_name,
  team_logo_url as avatar
from yahoo_data.teams
order by manager_1_id::int, team_key, inserted_timestamp desc;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_users as
with
league_users as (
  select
    ( split_part(team_key, '.', 1) || '.'
      || split_part(team_key, '.', 2) || '.'
      || split_part(team_key, '.', 3)
    ) as league_id,
    jsonb_build_object(
      'team_key', team_key,
      'user_id', user_id,
      'username', username,
      'metadata', metadata,
      'is_owner', is_owner,
      'is_bot', is_bot,
      'display_name', display_name,
      'avatar', avatar
    ) as league_user
  from public.view_league_user
)

select
  league_id,
  array_agg(league_user) as league_users
from league_users
group by league_id;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_transaction as
with
unpiv as (
  select distinct on (transaction_key)
    transaction_key,
    type,
    trader_team_key,
    tradee_team_key,
    unnest(
      array[
        coalesce(
          case
            when coalesce(player_destination_team_key_1, '') = '' then null else player_destination_team_key_1
          end,
          case
            when coalesce(player_destination_team_key_2, '') = '' then null else player_destination_team_key_2
          end),
        player_destination_team_key_2,
        player_destination_team_key_3,
        player_destination_team_key_4,
        player_destination_team_key_5,
        player_destination_team_key_6,
        player_destination_team_key_7,
        player_destination_team_key_8,
        player_destination_team_key_9,
        player_destination_team_key_10
      ]
    ) as player_destination_team_key,
    unnest(
      array[
        player_key_1,
        player_key_2,
        player_key_3,
        player_key_4,
        player_key_5,
        player_key_6,
        player_key_7,
        player_key_8,
        player_key_9,
        player_key_10
      ]
    ) as player_key,
    unnest(
      array[
        player_type_1,
        player_type_2,
        player_type_3,
        player_type_4,
        player_type_5,
        player_type_6,
        player_type_7,
        player_type_8,
        player_type_9,
        player_type_10
      ]
    ) as player_type
  from yahoo_data.transactions
  order by transaction_key, inserted_timestamp desc
),

trans as (
  select
    transaction_key,
    trader_team_key,
    tradee_team_key,
    player_destination_team_key,
    player_key,
    player_type
  from unpiv
  where coalesce(player_key, '') != ''
  union all
  select
    transaction_key,
    tradee_team_key as trader_team_key,
    trader_team_key as tradee_team_key,
    player_destination_team_key,
    player_key,
    player_type
  from unpiv
  where coalesce(player_key, '') != ''
    and coalesce(type, '') = 'trade'
),

trans_adds_drops as (
select
  transaction_key,
  case
    when player_type in ('add', 'drop')
      then player_type || 's'
    when trader_team_key = player_destination_team_key
      then 'adds'
    when tradee_team_key = player_destination_team_key
      then 'drops'
    end as add_drop,
  trader_team_key,
  tradee_team_key,
  player_destination_team_key,
  player_key,
  player_type
from trans
),

adds_drops as (
select
  transaction_key,
  add_drop,
  jsonb_object_agg(
    player_key, split_part(player_destination_team_key, '.', 5)
  ) as add_drop_array
from trans_adds_drops
group by transaction_key, add_drop
),

transactions as (
  select distinct on (trans.transaction_key)
    array[null::jsonb] as waiver_budget,
    trans.timestamp::int as transaction_updated,
    array_distinct(
      array_remove(
        array_remove(
          array[
            split_part(trans.player_destination_team_key_1, '.', 5),
            split_part(trans.player_destination_team_key_2, '.', 5),
            split_part(trans.player_destination_team_key_3, '.', 5),
            split_part(trans.player_destination_team_key_4, '.', 5),
            split_part(trans.player_destination_team_key_5, '.', 5),
            split_part(trans.player_destination_team_key_6, '.', 5),
            split_part(trans.player_destination_team_key_7, '.', 5),
            split_part(trans.player_destination_team_key_8, '.', 5),
            split_part(trans.player_destination_team_key_9, '.', 5),
            split_part(trans.player_destination_team_key_10, '.', 5)
          ], null
        ), ''
      )
    ) as roster_ids,
    -- jsonb as drops,
    array_distinct(
      array_remove(
        array_remove(
          array[
            split_part(trans.player_destination_team_key_1, '.', 5),
            split_part(trans.player_destination_team_key_2, '.', 5),
            split_part(trans.player_destination_team_key_3, '.', 5),
            split_part(trans.player_destination_team_key_4, '.', 5),
            split_part(trans.player_destination_team_key_5, '.', 5),
            split_part(trans.player_destination_team_key_6, '.', 5),
            split_part(trans.player_destination_team_key_7, '.', 5),
            split_part(trans.player_destination_team_key_8, '.', 5),
            split_part(trans.player_destination_team_key_9, '.', 5),
            split_part(trans.player_destination_team_key_10, '.', 5)
          ], null
        ), ''
      )
    ) as consenter_ids,
    -- jsonb as adds,
    trans.transaction_key as transaction_id,
    coalesce(
      case
        when coalesce(trans.player_destination_team_key_1, '') = '' then null else trans.player_destination_team_key_1
      end,
      case
        when coalesce(trans.player_destination_team_key_2, '') = '' then null else trans.player_destination_team_key_2
      end
    ) as creator,
    array[null::jsonb] as draft_picks,
    g_wks.game_week::int as leg,
    jsonb_build_object(
      'waiver_bid', faab_bid
    ) as settings,
    trans.timestamp::int as  created,
    null::jsonb as metadata,
    case
      when trans.type = 'trade' then 'trade'
      when trans.player_type_1 = 'drop'
        and trans.player_destination_type_1 in ('waivers', 'freeagents')
        and coalesce(trans.player_type_2, '') in ('add', '')
        and trans.player_source_type_2 = 'freeagents'
        then 'free_agent'
      when trans.player_type_1 = 'add'
        and trans.player_source_type_1 = 'freeagents'
        then 'free_agent'
      when trans.player_type_1 = 'drop'
        and coalesce(trans.player_type_2, '') in ('add', '')
        and trans.player_source_type_2 = 'waivers'
        then 'waiver'
      when trans.player_type_1 = 'drop'
        and trans.player_destination_type_1 = 'waivers'
        and coalesce(trans.player_type_2, '') = ''
        then 'free_agent'
      when trans.player_type_1 = 'add'
        and trans.player_source_type_1 = 'waivers'
        then 'waiver'
    end as type,
    trans.status,
    trans.league_key as league_id
  from yahoo_data.transactions trans
  left join yahoo_data.game_weeks g_wks
    on to_timestamp(trans.timestamp::int)::date between g_wks.game_week_start::date and g_wks.game_week_end::date
  order by trans.transaction_key, trans.inserted_timestamp desc
)

select
  trans.waiver_budget,
  trans.transaction_updated,
  trans.roster_ids,
  drops.add_drop_array as drops,
  trans.consenter_ids,
  adds.add_drop_array as adds,
  trans.transaction_id,
  trans.creator,
  trans.draft_picks,
  trans.leg,
  trans.settings,
  trans.created,
  trans.metadata,
  trans.type,
  case when trans.status = 'successful' then 'complete' else status end as status,
  trans.league_id
from transactions trans
left join (
  select
    transaction_key,
    add_drop_array
  from adds_drops
  where add_drop = 'adds'
) adds
  on adds.transaction_key = trans.transaction_id
left join (
  select
    transaction_key,
    add_drop_array
  from adds_drops
  where add_drop = 'drops'
) drops
  on drops.transaction_key = trans.transaction_id;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_transactions as
with
trans as (
  select
    jsonb_build_object(
      'waiver_budget', waiver_budget,
      'transaction_updated', transaction_updated,
      'roster_ids', roster_ids,
      'drops', drops,
      'consenter_ids', consenter_ids,
      'adds', adds,
      'transaction_id', transaction_id,
      'creator', creator,
      'draft_picks', draft_picks,
      'leg', leg,
      'settings', settings,
      'created', created,
      'metadata', metadata,
      'type', type,
      'status', status,
      'league_id', league_id
    ) as league_transaction,
    league_id
  from public.view_league_transaction
)

select
  league_id,
  array_agg(league_transaction) as league_transactions
from trans
group by league_id;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_player_data as
select distinct on (player_key)
  player_key as player_id,
  -- college varchar,
  -- weight varchar,
  -- practice_description varchar,
  -- espn_id bigint,
  editorial_team_full_name as team,
  -- sportradar_id varchar,
  full_name,
  -- injury_status varchar,
  -- injury_start_date varchar,
  primary_position as position,
  -- birth_state varchar,
  -- rotowire_id bigint,
  -- swish_id bigint,
  jsonb_build_object(
    'rookie_year', null
  ) as metadata,
  -- oddsjam_id varchar,
  uniform_number::int as number,
  injury_note as injury_notes,
  first_ascii_name as search_first_name,
  -- hashtag varchar,
  string_to_array(eligible_positions, ',') as fantasy_positions,
  last_name,
  -- injury_body_part varchar,
  -- birth_city varchar,
  -- practice_participation varchar,
  -- age int,
  -- birth_country varchar,
  -- stats_id bigint,
  -- gsis_id varchar,
  -- pandascore_id varchar,
  -- status varchar,
  last_ascii_name as search_last_name,
  -- high_school varchar,
  'nfl' as sport,
  -- rotoworld_id int,
  player_notes_last_timestamp::bigint as news_updated,
  -- birth_date varchar,
  first_name,
  true as active,
  -- fantasy_data_id int,
  -- depth_chart_order int,
  -- years_exp int,
  -- height varchar,
  -- search_full_name varchar,
  player_id::bigint as yahoo_id
  -- search_rank bigint,
  -- depth_chart_position varchar
from yahoo_data.player_pct_owned
order by player_key, inserted_timestamp desc;
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_settings as
with
league as (
  select distinct on (league_key)
    *
  from yahoo_data.leagues
  order by league_key, inserted_timestamp desc
),

settings as (
  select distinct on (league_key)
    *
  from yahoo_data.settings
  order by league_key, inserted_timestamp desc
)

select
  settings.waiver_time::int as daily_waivers_last_ran,
  1 as reserve_allow_cov,
  1 as reserve_slots,
  league.current_week::int as leg,
  0 as offseason_adds,
  0 as bench_lock,
  settings.trade_reject_time::int as trade_review_days,
  1 as league_average_match,
  case when settings.waiver_type = 'FR' then 2 else 0 end as waiver_type,
  0 as max_keepers,
  0 as type,
  0 as pick_trading,
  1 as daily_waivers,
  0 as taxi_years,
  13 as trade_deadline,
  0 as reserve_allow_sus,
  1 as reserve_allow_out,
  0 as playoff_round_type,
  2 as waiver_day_of_week,
  0 as taxi_allow_vets,
  0 as reserve_allow_dnr,
  0 as commissioner_direct_invite,
  0 as reserve_allow_doubtful,
  1 as waiver_clear_days,
  settings.playoff_start_week::int as playoff_week_start,
  5461 as daily_waivers_days,
  league.current_week::int - 1 as last_scored_leg,
  0 as taxi_slots,
  0 as playoff_type,
  9 as daily_waivers_hour,
  settings.max_teams::int as num_teams,
  settings.num_playoff_teams::int as playoff_teams,
  settings.uses_playoff_reseeding::int as playoff_seed_type,
  league.start_week::int as start_week,
  0 as reserve_allow_na,
  15 as draft_rounds,
  0 as taxi_deadline,
  0 as waiver_bid_min,
  0 as capacity_override,
  0 as disable_adds,
  100 as waiver_budget,
  league.current_week::int - 1 as last_report,
  league.league_key as league_id
from league
join settings
  on settings.league_key = league.league_key;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_scoring_settings_unpivoted as
with
stat_mod as (
  select distinct on (league_key, stat_id)
    split_part(league_key, '.', 1) as game_key,
    league_key,
    stat_id,
    value::decimal(4, 2) as stat_modifier
  from yahoo_data.stat_modifiers
  order by league_key, stat_id, inserted_timestamp desc
),

stat_cat_init as (
  select distinct on (coalesce(game_key, split_part(league_key, '.', 1)), stat_id)
    coalesce(game_key, split_part(league_key, '.', 1)) as game_key,
    stat_id,
    name,
    regexp_replace(lower(display_name), '[^a-z0-9_]', '_', 'g') as display_name,
    abbr,
    is_enabled,
    enabled,
    is_excluded_from_display,
    is_only_display_stat,
    position_type,
    sort_order,
    stat_group
  from yahoo_data.stat_categories
  order by coalesce(game_key, split_part(league_key, '.', 1)), stat_id, inserted_timestamp desc
),

stat_cat as (
  select
    sc1.game_key,
    sc1.stat_id,
    sc1.name,
    sc1.display_name,
    coalesce(sc1.abbr, sc2.abbr) as abbr,
    sc1.is_enabled,
    sc1.enabled,
    sc1.is_excluded_from_display,
    sc1.is_only_display_stat,
    sc1.position_type,
    sc1.sort_order,
    sc1.stat_group
  from stat_cat_init sc1
  left join (
    select distinct
      stat_id, abbr
    from stat_cat_init
    where coalesce(abbr, '') != ''
  ) sc2
    on sc2.stat_id::int = sc1.stat_id::int
)

select
  stat_cat.game_key,
  stat_mod.league_key,
  stat_cat.display_name,
  coalesce(stat_mod.stat_modifier, 0.00) as stat_modifier
from stat_cat
left join stat_mod
  on stat_mod.game_key::int = stat_cat.game_key::int
  and stat_mod.stat_id::int = stat_cat.stat_id::int;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_scoring_settings as
select
  game_key,
  max(league_key) as league_key,
  max("2_pt") as "rush_2pt",
  max("2_pt") as "pass_2pt",
  max("2_pt") as "rec_2pt",
  -- max("3_and_outs") as "3_and_outs",
  -- max("40_yd_comp") as "40_yd_comp",
  -- max("40_yd_pass_td") as "40_yd_pass_td",
  -- max("40_yd_rec") as "40_yd_rec",
  -- max("40_yd_rec_td") as "40_yd_rec_td",
  -- max("40_yd_rush") as "40_yd_rush",
  -- max("40_yd_rush_td") as "40_yd_rush_td",
  -- max("4_dwn_stops") as "4_dwn_stops",
  max("blk_kick") as "blk_kick",
  -- max("comp") as "comp",
  -- max("def_yds_allow") as "def_yds_allow",
  -- max("fg_0_19") as "fg_0_19",
  -- max("fg_20_29") as "fg_20_29",
  -- max("fg_30_39") as "fg_30_39",
  -- max("fg_40_49") as "fg_40_49",
  -- max("fg_50_") as "fg_50_",
  -- max("fg_made") as "fg_made",
  max("fg_miss") as "fgmiss",
  max("fg_yds") as "fgm_yds",
  max("fgm_0_19") as "fgm_0_19",
  max("fgm_20_29") as "fgm_20_29",
  max("fgm_30_39") as "fgm_30_39",
  max("fgm_40_49") as "fgm_40_49",
  max("fgm_50_") as "fgm_50p",
  max("fum") as "fum",
  max("fum_force") as "ff",
  max("fum_force") as "def_st_ff",
  max("fum_force") as "st_ff",
  max("fum_lost") as "fum_lost",
  max("fum_rec") as "st_fum_rec",
  max("fum_rec") as "def_st_fum_rec",
  max("fum_rec") as "fum_rec",
  max("fum_ret_td") as "fum_rec_td",
  -- max("gp") as "gp",
  -- max("inc") as "inc",
  max("int") as "pass_int",
  max("int") as "int",
  -- max("pass_1st_downs") as "pass_1st_downs",
  -- max("pass_att") as "pass_att",
  -- max("pass_def") as "pass_def",
  max("pass_td") as "pass_td",
  max("pass_yds") as "pass_yd",
  max("pat_made") as "xpm",
  max("pat_miss") as "xpmiss",
  -- max("pick_six") as "pick_six",
  -- max("pts_allow") as "pts_allow",
  max("pts_allow_0") as "pts_allow_0",
  max("pts_allow_14_20") as "pts_allow_14_20",
  max("pts_allow_1_6") as "pts_allow_1_6",
  max("pts_allow_21_27") as "pts_allow_21_27",
  max("pts_allow_28_34") as "pts_allow_28_34",
  max("pts_allow_35_") as "pts_allow_35p",
  max("pts_allow_7_13") as "pts_allow_7_13",
  max("rec") as "rec",
  -- max("rec_1st_downs") as "rec_1st_downs",
  max("rec_td") as "rec_td",
  max("rec_yds") as "rec_yd",
  -- max("ret_td") as "st_td",
  -- max("ret_yds") as "ret_yds",
  -- max("rush_1st_downs") as "rush_1st_downs",
  -- max("rush_att") as "rush_att",
  max("rush_td") as "rush_td",
  max("rush_yds") as "rush_yd",
  max("sack") as "sack",
  max("safe") as "safe",
  -- max("tack_ast") as "tack_ast",
  -- max("tack_solo") as "tack_solo",
  -- max("targets") as "targets",
  -- max("td") as "td",
  max("td") as "def_st_td",
  max("td") as "def_td"
  -- ,max("tfl") as "tfl",
  -- max("to_ret_yds") as "to_ret_yds",
  -- max("xpr") as "xpr",
  -- max("yds_allow_0_99") as "yds_allow_0_99",
  -- max("yds_allow_100_199") as "yds_allow_100_199",
  -- max("yds_allow_200_299") as "yds_allow_200_299",
  -- max("yds_allow_300_399") as "yds_allow_300_399",
  -- max("yds_allow_400_499") as "yds_allow_400_499",
  -- max("yds_allow_500_") as "yds_allow_500_",
  -- max("yds_allow_neg") as "yds_allow_neg"
from crosstab(
  'select game_key, league_key, display_name, stat_modifier from public.view_league_scoring_settings_unpivoted order by 3',
  'select distinct display_name from public.view_league_scoring_settings_unpivoted'
) as ct(
  game_key text,
  league_key text,
  "2_pt" decimal(4, 2),
  "3_and_outs" decimal(4, 2),
  "40_yd_comp" decimal(4, 2),
  "40_yd_pass_td" decimal(4, 2),
  "40_yd_rec" decimal(4, 2),
  "40_yd_rec_td" decimal(4, 2),
  "40_yd_rush" decimal(4, 2),
  "40_yd_rush_td" decimal(4, 2),
  "4_dwn_stops" decimal(4, 2),
  "blk_kick" decimal(4, 2),
  "comp" decimal(4, 2),
  "def_yds_allow" decimal(4, 2),
  "fg_0_19" decimal(4, 2),
  "fg_20_29" decimal(4, 2),
  "fg_30_39" decimal(4, 2),
  "fg_40_49" decimal(4, 2),
  "fg_50_" decimal(4, 2),
  "fg_made" decimal(4, 2),
  "fg_miss" decimal(4, 2),
  "fg_yds" decimal(4, 2),
  "fgm_0_19" decimal(4, 2),
  "fgm_20_29" decimal(4, 2),
  "fgm_30_39" decimal(4, 2),
  "fgm_40_49" decimal(4, 2),
  "fgm_50_" decimal(4, 2),
  "fum" decimal(4, 2),
  "fum_force" decimal(4, 2),
  "fum_lost" decimal(4, 2),
  "fum_rec" decimal(4, 2),
  "fum_ret_td" decimal(4, 2),
  "gp" decimal(4, 2),
  "inc" decimal(4, 2),
  "int" decimal(4, 2),
  "pass_1st_downs" decimal(4, 2),
  "pass_att" decimal(4, 2),
  "pass_def" decimal(4, 2),
  "pass_td" decimal(4, 2),
  "pass_yds" decimal(4, 2),
  "pat_made" decimal(4, 2),
  "pat_miss" decimal(4, 2),
  "pick_six" decimal(4, 2),
  "pts_allow" decimal(4, 2),
  "pts_allow_0" decimal(4, 2),
  "pts_allow_14_20" decimal(4, 2),
  "pts_allow_1_6" decimal(4, 2),
  "pts_allow_21_27" decimal(4, 2),
  "pts_allow_28_34" decimal(4, 2),
  "pts_allow_35_" decimal(4, 2),
  "pts_allow_7_13" decimal(4, 2),
  "rec" decimal(4, 2),
  "rec_1st_downs" decimal(4, 2),
  "rec_td" decimal(4, 2),
  "rec_yds" decimal(4, 2),
  "ret_td" decimal(4, 2),
  "ret_yds" decimal(4, 2),
  "rush_1st_downs" decimal(4, 2),
  "rush_att" decimal(4, 2),
  "rush_td" decimal(4, 2),
  "rush_yds" decimal(4, 2),
  "sack" decimal(4, 2),
  "safe" decimal(4, 2),
  "tack_ast" decimal(4, 2),
  "tack_solo" decimal(4, 2),
  "targets" decimal(4, 2),
  "td" decimal(4, 2),
  "tfl" decimal(4, 2),
  "to_ret_yds" decimal(4, 2),
  "xpr" decimal(4, 2),
  "yds_allow_0_99" decimal(4, 2),
  "yds_allow_100_199" decimal(4, 2),
  "yds_allow_200_299" decimal(4, 2),
  "yds_allow_300_399" decimal(4, 2),
  "yds_allow_400_499" decimal(4, 2),
  "yds_allow_500_" decimal(4, 2),
  "yds_allow_neg" decimal(4, 2)
)
group by game_key;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league as
select
  league.league_key as league_id,
  league.num_teams as total_rosters,
  league.league_key || '.draft' as draft_id,
  league.name,
  null::text as company_id,
  league.season,
  'regular' as season_type,
  league.game_code as sport,
  0 as shard,
  league.logo_url as avatar,
  null::varchar as last_message_id,
  null::varchar as last_read_id,
  null::varchar as last_pinned_message_id,
  null::int as last_message_time,
  null::varchar as last_message_text_map,
  null::varchar as last_message_attachment,
  null::boolean as last_author_is_bot,
  null::varchar as last_author_id,
  null::varchar as last_author_display_name,
  null::varchar as last_author_avatar,
  league.renew as previous_league_id,
  case when league.is_finished::int = 1 then 'complete' else 'in_season' end as status,
  case
    when league.season::int >= 2023 then array['QB', 'RB', 'RB', 'WR', 'WR', 'TE', 'FLEX', 'FLEX', 'DEF', 'BN', 'BN', 'BN', 'BN', 'BN', 'BN', 'IR']
    when league.season::int = 2020 then array['QB', 'RB', 'RB', 'WR', 'WR', 'TE', 'FLEX', 'K', 'DEF', 'BN', 'BN', 'BN', 'BN', 'BN', 'BN', 'IR', 'IR']
    else array['QB', 'RB', 'RB', 'WR', 'WR', 'TE', 'FLEX', 'K', 'DEF', 'BN', 'BN', 'BN', 'BN', 'BN', 'BN', 'IR']
    end as roster_positions,
  null::int as group_id,
  league.league_key || '.winners_bracket' as bracket_id,
  league.league_key || '.losers_bracket' as loser_bracket_id,
  jsonb_build_object(
    'rush_2pt', league_score."rush_2pt",
    'pass_2pt', league_score."pass_2pt",
    'rec_2pt', league_score."rec_2pt",
    'blk_kick', league_score."blk_kick",
    'fgmiss', league_score."fgmiss",
    'fgm_yds', league_score."fgm_yds",
    'fgm_0_19', league_score."fgm_0_19",
    'fgm_20_29', league_score."fgm_20_29",
    'fgm_30_39', league_score."fgm_30_39",
    'fgm_40_49', league_score."fgm_40_49",
    'fgm_50p', league_score."fgm_50p",
    'fum', league_score."fum",
    'ff', league_score."ff",
    'def_st_ff', league_score."def_st_ff",
    'st_ff', league_score."st_ff",
    'fum_lost', league_score."fum_lost",
    'st_fum_rec', league_score."st_fum_rec",
    'def_st_fum_rec', league_score."def_st_fum_rec",
    'fum_rec', league_score."fum_rec",
    'fum_rec_td', league_score."fum_rec_td",
    'pass_int', league_score."pass_int",
    'int', league_score."int",
    'pass_td', league_score."pass_td",
    'pass_yd', league_score."pass_yd",
    'xpm', league_score."xpm",
    'xpmiss', league_score."xpmiss",
    'pts_allow_0', league_score."pts_allow_0",
    'pts_allow_1_6', league_score."pts_allow_1_6",
    'pts_allow_7_13', league_score."pts_allow_7_13",
    'pts_allow_14_20', league_score."pts_allow_14_20",
    'pts_allow_21_27', league_score."pts_allow_21_27",
    'pts_allow_28_34', league_score."pts_allow_28_34",
    'pts_allow_35p', league_score."pts_allow_35p",
    'rec', league_score."rec",
    'rec_td', league_score."rec_td",
    'rec_yd', league_score."rec_yd",
    'rush_td', league_score."rush_td",
    'rush_yd', league_score."rush_yd",
    'sack', league_score."sack",
    'safe', league_score."safe",
    'def_st_td', league_score."def_st_td",
    'def_td', league_score."def_td"
  ) as scoring_settings,
  jsonb_build_object(
    'daily_waivers_last_ran', league_sett.daily_waivers_last_ran,
    'reserve_allow_cov', league_sett.reserve_allow_cov,
    'reserve_slots', league_sett.reserve_slots,
    'leg', league_sett.leg,
    'offseason_adds', league_sett.offseason_adds,
    'bench_lock', league_sett.bench_lock,
    'trade_review_days', league_sett.trade_review_days,
    'league_average_match', league_sett.league_average_match,
    'waiver_type', league_sett.waiver_type,
    'max_keepers', league_sett.max_keepers,
    'type', league_sett.type,
    'pick_trading', league_sett.pick_trading,
    'daily_waivers', league_sett.daily_waivers,
    'taxi_years', league_sett.taxi_years,
    'trade_deadline', league_sett.trade_deadline,
    'reserve_allow_sus', league_sett.reserve_allow_sus,
    'reserve_allow_out', league_sett.reserve_allow_out,
    'playoff_round_type', league_sett.playoff_round_type,
    'waiver_day_of_week', league_sett.waiver_day_of_week,
    'taxi_allow_vets', league_sett.taxi_allow_vets,
    'reserve_allow_dnr', league_sett.reserve_allow_dnr,
    'commissioner_direct_invite', league_sett.commissioner_direct_invite,
    'reserve_allow_doubtful', league_sett.reserve_allow_doubtful,
    'waiver_clear_days', league_sett.waiver_clear_days,
    'playoff_week_start', league_sett.playoff_week_start,
    'daily_waivers_days', league_sett.daily_waivers_days,
    'last_scored_leg', league_sett.last_scored_leg,
    'taxi_slots', league_sett.taxi_slots,
    'playoff_type', league_sett.playoff_type,
    'daily_waivers_hour', league_sett.daily_waivers_hour,
    'num_teams', league_sett.num_teams,
    'playoff_teams', league_sett.playoff_teams,
    'playoff_seed_type', league_sett.playoff_seed_type,
    'start_week', league_sett.start_week,
    'reserve_allow_na', league_sett.reserve_allow_na,
    'draft_rounds', league_sett.draft_rounds,
    'taxi_deadline', league_sett.taxi_deadline,
    'waiver_bid_min', league_sett.waiver_bid_min,
    'capacity_override', league_sett.capacity_override,
    'disable_adds', league_sett.disable_adds,
    'waiver_budget', league_sett.waiver_budget,
    'last_report', league_sett.last_report,
    'league_id', league_sett.league_id
  ) as settings,
  jsonb_build_object(
    'keeper_deadline', 0
  ) as metadata
from yahoo_data.view_leagues league
left join public.view_league_scoring_settings league_score
  on league_score.league_key = league.league_key
left join  public.view_league_settings league_sett
  on league_sett.league_id = league.league_key;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_draft_metadata as
select
  league_key,
  'half_ppr' as scoring_type,
  name,
  '' as description,
  draft_id
from yahoo_data.view_leagues;
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_draft_picks as
with
draft_res as (
  select distinct on (pick::int)
    league_key || '.draft' as draft_id,
    pick::int as pick_no,
    round::int as round,
    player_key as player_id,
    team_key as picked_by
  from yahoo_data.draft_results
  where left(league_key, 3) = '423'
  order by pick::int, inserted_timestamp desc
),

metadata as (
  select
    player_id,
    jsonb_build_object(
      'years_exp', years_exp,
      'team', team,
      'status', status,
      'sport', sport,
      'player_id', player_id,
      'number', number,
      'news_updated', news_updated,
      'last_name', last_name,
      'injury_status', injury_status,
      'first_name', first_name
    ) as metadata
  from public.view_draft_picks_player_metadata
),

managers as (
  select distinct on (team_key)
    t.team_key,
    t.team_id::int as roster_id,
    d.pick_no as draft_slot
  from yahoo_data.teams t
  left join (
    select
      picked_by,
      pick_no
    from draft_res
    where pick_no <= 10
  ) d
  on d.picked_by = t.team_key
  where left(team_key, 3) = '423'
  order by t.team_key, t.inserted_timestamp desc
)

select
    draft_res.round,
    managers.roster_id,
    draft_res.player_id,
    draft_res.picked_by,
    draft_res.pick_no,
    metadata.metadata,
    false as is_keeper,
    managers.draft_slot,
    draft_res.draft_id
from draft_res
left join managers
  on managers.team_key = draft_res.picked_by
left join metadata
  on metadata.player_id = draft_res.player_id
order by pick_no;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_draft_settings as
select
  league_key,
  num_teams::int as teams,
  2 as slots_wr,
  1 as slots_te,
  2 as slots_rb,
  1 as slots_qb,
  2 as slots_flex,
  1 as slots_def,
  6 as slots_bn,
  (2 + 1 + 2 + 1 + 2 + 1 + 6) as rounds,
  0 as reversal_round,
  0 as player_type,
  draft_pick_time::int as pick_timer,
  60 as nomination_timer,
  1 as cpu_autopick,
  0 as autostart,
  240 as autopause_start_time,
  960 as autopause_end_time,
  0 as autopause_enabled,
  0 as alpha_sort,
  draft_id
from  yahoo_data.view_leagues;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_draft as
with
draft_res as (
  select distinct on (league_key, pick::int)
    league_key || '.draft' as draft_id,
    pick::int as pick_no,
    round::int as round,
    player_key as player_id,
    team_key as picked_by
  from yahoo_data.draft_results
  where left(league_key, 3) = '423'
  order by league_key, pick::int, inserted_timestamp desc
),

slot_to_roster as (
  select
    draft_id,
    jsonb_object_agg(pick_no, team_id) as slot_to_roster_id
  from (
    select distinct on (t.league_key, t.team_id::int)
      t.league_key || '.draft' as draft_id,
      t.team_id,
      d.pick_no::text as pick_no
    from yahoo_data.teams t
    left join (
      select
        picked_by,
        pick_no
      from draft_res
      where pick_no <= 10
    ) d
    on d.picked_by = t.team_key
    where left(team_key, 3) = '423'
    order by t.league_key, t.team_id::int, t.inserted_timestamp desc
  ) as slot_rost
  group by draft_id
),

draft_settings as (
  select
    league_key,
    draft_id,
    jsonb_build_object(
      'teams', teams,
      'slots_wr', slots_wr,
      'slots_te', slots_te,
      'slots_rb', slots_rb,
      'slots_qb', slots_qb,
      'slots_flex', slots_flex,
      'slots_def', slots_def,
      'slots_bn', slots_bn,
      'rounds', rounds,
      'reversal_round', reversal_round,
      'player_type', player_type,
      'pick_timer', pick_timer,
      'nomination_timer', nomination_timer,
      'cpu_autopick', cpu_autopick,
      'autostart', autostart,
      'autopause_start_time', autopause_start_time,
      'autopause_end_time', autopause_end_time,
      'autopause_enabled', autopause_enabled,
      'alpha_sort', alpha_sort
    ) as settings
  from public.view_draft_settings
),

draft_meta as (
  select
    draft_id,
    jsonb_build_object(
      'scoring_type', scoring_type,
      'name', name,
      'description', description
    ) as metadata
  from public.view_draft_metadata
),

draft_ord as (
  select
    draft_id,
    jsonb_object_agg(team_key, pick_no) as draft_order
  from (
    select distinct on (t.league_key, team_key)
      t.league_key || '.draft' as draft_id,
      t.team_key,
      d.pick_no::text
    from yahoo_data.teams t
    left join (
      select
        picked_by,
        pick_no
      from draft_res
      where pick_no <= 10
    ) d
    on d.picked_by = t.team_key
    where left(team_key, 3) = '423'
    order by t.league_key, t.team_key, t.inserted_timestamp desc
  ) as df_ord
  group by draft_id
),

draft_creator as (
  select distinct on (league_key)
    array[league_key || '.t.1'] as creators,
    league_key || '.draft' as draft_id
  from yahoo_data.leagues
  order by league_key, inserted_timestamp desc
),

draft_base as (
  select
    draft_type as type,
    case when draft_status = 'postdraft' then 'complete' else draft_status end as status,
    draft_time::int as start_time,
    game_code as sport,
    'regular' as season_type,
    season,
    league_key as league_id,
    0 as last_picked,
    '' as last_message_id,
    draft_id,
    league_update_timestamp as created
  from yahoo_data.view_leagues
)

select
  draft_settings.league_key,
  draft_base.type,
  draft_base.status,
  draft_base.start_time,
  draft_base.sport,
  slot_to_roster.slot_to_roster_id,
  draft_settings.settings,
  draft_base.season_type,
  draft_base.season,
  draft_meta.metadata,
  draft_base.league_id,
  draft_base.last_picked,
  draft_base.last_message_id,
  draft_ord.draft_order,
  draft_base.draft_id,
  draft_creator.creators,
  draft_base.created
from draft_base
left join slot_to_roster
  on slot_to_roster.draft_id = draft_base.draft_id
left join draft_settings
  on draft_settings.draft_id = draft_base.draft_id
left join draft_meta
  on draft_meta.draft_id = draft_base.draft_id
left join draft_ord
  on draft_ord.draft_id = draft_base.draft_id
left join draft_creator
  on draft_creator.draft_id = draft_base.draft_id;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_drafts as
select
  league_key as league_id,
  array[
    jsonb_build_object(
      'type', type,
      'status', status,
      'start_time', start_time,
      'sport', sport,
      'slot_to_roster_id', slot_to_roster_id,
      'settings', settings,
      'season_type', season_type,
      'season', season,
      'metadata', metadata,
      'league_id', league_id,
      'last_picked', last_picked,
      'last_message_id', last_message_id,
      'draft_order', draft_order,
      'draft_id', draft_id,
      'creators', creators,
      'created', created
    )
  ] as league_drafts
from public.view_draft;
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_winners_brackets as
select
  league_id,
  array_agg(bracket) as bracket
from (
  select
    league_key as league_id,
    jsonb_build_object(
      't1', t1,
      't2', t2,
      'w', w,
      'l', l,
      'r', r,
      'm', m
    ) as bracket
  from public.view_league_postseason_round_one
  where bracket_type = 'winners'
  union all
  select
    league_key as league_id,
    jsonb_build_object(
      't1', t1,
      't1_from', t1_from,
      't2', t2,
      't2_from', t2_from,
      'w', w,
      'l', l,
      'r', r,
      'm', m,
      'p', p
    ) as bracket
  from public.view_league_postseason_round_two
  where bracket_type = 'winners'
) loser_braket
group by league_id;
--------------------------------------------------------------------------------------------------------------------------------
create or replace view public.view_league_losers_brackets as
select
  league_id,
  array_agg(bracket) as bracket
from (
  select
    league_key as league_id,
    jsonb_build_object(
      't1', t1,
      't2', t2,
      'w', w,
      'l', l,
      'r', r,
      'm', m
    ) as bracket
  from public.view_league_postseason_round_one
  where bracket_type = 'losers'
  union all
  select
    league_key as league_id,
    jsonb_build_object(
      't1', t1,
      't1_from', t1_from,
      't2', t2,
      't2_from', t2_from,
      'w', w,
      'l', l,
      'r', r,
      'm', m,
      'p', p
    ) as bracket
  from public.view_league_postseason_round_two
  where bracket_type = 'losers'
) loser_braket
group by league_id;
--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------
