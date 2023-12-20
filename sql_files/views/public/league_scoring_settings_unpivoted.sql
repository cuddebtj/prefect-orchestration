create or replace view public.view_league_scoring_settings_unpivoted as
with
stat_mod as (
  select distinct on (league_key, stat_id)
    split_part(league_key, '.', 1) as game_key,
    league_key,
    stat_id,
    value::decimal(4, 2) as stat_modifier
  from yahoo_data.stat_modifiers
  order by league_key, stat_id, inserted_timestamp desc
),

stat_cat_init as (
  select distinct on (coalesce(game_key, split_part(league_key, '.', 1)), stat_id)
    coalesce(game_key, split_part(league_key, '.', 1)) as game_key,
    stat_id,
    name,
    regexp_replace(lower(display_name), '[^a-z0-9_]', '_', 'g') as display_name,
    abbr,
    is_enabled,
    enabled,
    is_excluded_from_display,
    is_only_display_stat,
    position_type,
    sort_order,
    stat_group
  from yahoo_data.stat_categories
  order by coalesce(game_key, split_part(league_key, '.', 1)), stat_id, inserted_timestamp desc
),

stat_cat as (
  select
    sc1.game_key,
    sc1.stat_id,
    sc1.name,
    sc1.display_name,
    coalesce(sc1.abbr, sc2.abbr) as abbr,
    sc1.is_enabled,
    sc1.enabled,
    sc1.is_excluded_from_display,
    sc1.is_only_display_stat,
    sc1.position_type,
    sc1.sort_order,
    sc1.stat_group
  from stat_cat_init sc1
  left join (
    select distinct
      stat_id, abbr
    from stat_cat_init
    where coalesce(abbr, '') != ''
  ) sc2
    on sc2.stat_id::int = sc1.stat_id::int
)

select
  stat_cat.game_key,
  stat_mod.league_key,
  stat_cat.display_name,
  coalesce(stat_mod.stat_modifier, 0.00) as stat_modifier
from stat_cat
left join stat_mod
  on stat_mod.game_key::int = stat_cat.game_key::int
  and stat_mod.stat_id::int = stat_cat.stat_id::int;

