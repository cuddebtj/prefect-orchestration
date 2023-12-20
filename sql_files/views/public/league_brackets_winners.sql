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
