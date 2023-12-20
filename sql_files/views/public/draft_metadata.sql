create or replace view public.view_draft_metadata as
select
  league_key,
  'half_ppr' as scoring_type,
  name,
  '' as description,
  draft_id
from yahoo_data.view_leagues;
