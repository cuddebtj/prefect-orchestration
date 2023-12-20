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
