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
