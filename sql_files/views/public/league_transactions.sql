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
