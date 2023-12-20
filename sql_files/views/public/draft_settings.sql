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
