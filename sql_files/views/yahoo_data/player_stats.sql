create or replace view yahoo_data.view_player_stats as
with
stat_mod as (
  select distinct on (stat_id)
    stat_id,
    value::decimal(8, 2) modifier
  from yahoo_data.stat_modifiers
  order by stat_id, inserted_timestamp desc
),

stat_cat as (
  select distinct on (stat_id)
    stat_cat.stat_id,
    stat_cat.display_name,
    stat_mod.modifier
  from yahoo_data.stat_categories stat_cat
  left join stat_mod
    on stat_mod.stat_id = stat_cat.stat_id
  order by stat_id, inserted_timestamp desc
),

player_map as (
  select distinct on (player_key)
    player_key,
    full_name
  from yahoo_data.players
  where coalesce(player_key, '') != ''
  order by player_key, inserted_timestamp desc
),

player_stats as (
  select distinct on (week, player_key, stat_id)
    week::decimal(2,0) week,
    player_key,
    stat_id,
    stat_value::decimal(8, 0) stat_value,
    total_points::decimal(8, 2) total_points
  from yahoo_data.player_stats
  order by week, player_key, stat_id, inserted_timestamp desc
),

final_stats as (
  select
    player_stats.week,
    player_stats.player_key,
    player_map.full_name,
    player_stats.stat_id,
    stat_cat.display_name,
    player_stats.stat_value,
    coalesce(stat_cat.modifier, 0.00) stat_modifier,
    (coalesce(stat_cat.modifier, 0.00) * player_stats.stat_value)::decimal(8, 2) fantasy_points,
    player_stats.total_points
  from player_stats
  left join stat_cat
    on stat_cat.stat_id = player_stats.stat_id
  left join player_map
    on player_map.player_key = player_stats.player_key
)

select *
from final_stats
order by week, full_name, stat_value desc;
