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
