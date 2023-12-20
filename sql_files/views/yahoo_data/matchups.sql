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
