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
