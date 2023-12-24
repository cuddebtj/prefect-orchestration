drop table yahoo_json.all_game_keys;
create table if not exists yahoo_json.all_game_keys(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.game;
create table if not exists yahoo_json.game(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.league_draft_result;
create table if not exists yahoo_json.league_draft_result(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.league_offseason;
create table if not exists yahoo_json.league_offseason(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.league_preseason;
create table if not exists yahoo_json.league_preseason(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.league_matchup;
create table if not exists yahoo_json.league_matchup(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.league_transaction;
create table if not exists yahoo_json.league_transaction(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.player;
create table if not exists yahoo_json.player(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.player_draft_analysis;
create table if not exists yahoo_json.player_draft_analysis(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.player_pct_owned;
create table if not exists yahoo_json.player_pct_owned(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.player_stat;
create table if not exists yahoo_json.player_stat(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
drop table yahoo_json.roster;
create table if not exists yahoo_json.roster(
    yahoo_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);


create table if not exists yahoo_json.sleeper_player_projections(
    sleeper_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
create table if not exists yahoo_json.sleeper_player_info(
    sleeper_json jsonb,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp
) WITH (fillfactor = 100);
