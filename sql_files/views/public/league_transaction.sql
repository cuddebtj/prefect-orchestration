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
