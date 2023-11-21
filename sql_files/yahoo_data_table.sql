create table if not exists yahoo_data.allgames(
    code text,
    game_id text,
    game_key text,
    is_game_over text,
    is_live_draft_lobby_active text,
    is_offseason text,
    is_registration_over text,
    name text,
    season text,
    type text,
    url text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_all_game primary key(game_id, inserted_timestamp)
);
create table if not exists yahoo_data.games(
    code text,
    game_id text,
    game_key text,
    name text,
    type text,
    url text,
    is_game_over text,
    is_offseason text,
    is_regitextation_over text,
    season text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_game_id primary key(game_id, inserted_timestamp)
);
create table if not exists yahoo_data.game_weeks(
    display_name text,
    game_id text,
    game_week text,
    game_week_end text,
    game_week_start text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_game_week primary key(game_id, game_week, inserted_timestamp)
);
create table if not exists yahoo_data.leagues(
    allow_add_to_dl_extra_pos text,
    current_week text,
    draft_status text,
    edit_key text,
    felo_tier text,
    game_code text,
    iris_group_chat_id text,
    is_cash_league text,
    is_finished text,
    is_plus_league text,
    is_pro_league text,
    end_date text,
    end_week text,
    league_id text,
    league_key text,
    name text,
    password text,
    start_date text,
    start_week text,
    league_type text,
    league_update_timestamp text,
    url text,
    logo_url text,
    num_teams text,
    renew text,
    renewed text,
    scoring_type text,
    season text,
    short_invitation_url text,
    weekly_deadline text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_league_key primary key(league_key, inserted_timestamp)
);
create table if not exists yahoo_data.settings(
    cant_cut_list text,
    draft_pick_time text,
    draft_time text,
    draft_together text,
    draft_type text,
    has_multiweek_championship text,
    has_playoff_consolation_games text,
    is_auction_draft text,
    is_publicly_viewable text,
    league_key text,
    max_teams text,
    max_weekly_adds text,
    num_playoff_consolation_teams text,
    num_playoff_teams text,
    persistent_url text,
    pickem_enabled text,
    player_pool text,
    playoff_start_week text,
    post_draft_players text,
    scoring_type text,
    sendbird_channel_url text,
    trade_end_date text,
    trade_ratify_type text,
    trade_reject_time text,
    uses_faab text,
    uses_fractional_points text,
    uses_lock_eliminated_teams text,
    uses_median_score text,
    uses_negative_points text,
    uses_playoff text,
    uses_playoff_reseeding text,
    waiver_rule text,
    waiver_time text,
    waiver_type text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_settings primary key(league_key, inserted_timestamp)
);
create table if not exists yahoo_data.stat_modifiers(
    league_key text,
    stat_id text,
    value text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_stat_mod primary key(league_key, stat_id, inserted_timestamp)
);
create table if not exists yahoo_data.stat_groups(
    group_abbr text,
    group_display_name text,
    group_name text,
    league_key text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_stat_group primary key(league_key, group_abbr, inserted_timestamp)
);
create table if not exists yahoo_data.stat_categories(
    abbr text,
    display_name text,
    game_key text,
    is_enabled text,
    is_excluded_from_display text,
    is_only_display_stat text,
    league_key text,
    position_type text,
    sort_order text,
    name text,
    stat_group text,
    stat_id text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_stat_category primary key(league_key, stat_id, inserted_timestamp)
);
create table if not exists yahoo_data.position_types(
    display_name text,
    game_key text,
    type text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_position_type primary key(game_key, type, inserted_timestamp)
);
create table if not exists yahoo_data.roster_positions(
    abbreviation text,
    display_name text,
    game_key text,
    is_bench text,
    is_disabled_list text,
    is_starting_position text,
    league_key text,
    position text,
    position_type text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_roster_position primary key(league_key, position, inserted_timestamp)
);
create table if not exists yahoo_data.teams(
    clinched_playoffs text,
    draft_grade text,
    draft_position text,
    draft_recap_url text,
    faab_balance text,
    has_draft_grade text,
    is_owned_by_current_login text,
    league_key text,
    league_scoring_type text,
    manager_1_felo_score text,
    manager_1_felo_tier text,
    manager_1_guid text,
    manager_1_id text,
    manager_1_name text,
    manager_2_felo_score text,
    manager_2_felo_tier text,
    manager_2_guid text,
    manager_2_id text,
    manager_2_name text,
    number_of_moves text,
    number_of_trades text,
    roster_adds text,
    team_id text,
    team_key text,
    team_logo_url text,
    name text,
    url text,
    week text constraint week_default default '0',
    waiver_priority text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_team primary key(week, team_key, inserted_timestamp)
);
create table if not exists yahoo_data.players(
    editorial_player_key text,
    first_ascii_name text,
    first_name text,
    full_name text,
    headshot_url text,
    is_keeper_cost text,
    is_keeper_kept text,
    is_keeper_status text,
    last_ascii_name text,
    last_name text,
    league_key text,
    player_id text,
    player_key text,
    player_url text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_player primary key(player_key, inserted_timestamp)
);
create table if not exists yahoo_data.matchups(
    is_consolation text,
    is_matchup_recap_available text,
    is_playoffs text,
    is_tied text,
    league_key text,
    matchup_grade_1_grade text,
    matchup_grade_1_team_key text,
    matchup_grade_2_grade text,
    matchup_grade_2_team_key text,
    matchup_recap_title text,
    matchup_recap_url text,
    status text,
    week text constraint week_default default '0',
    team_1_key text,
    team_1_points text,
    team_1_projected_points text,
    team_1_win_probability text,
    team_2_key text,
    team_2_points text,
    team_2_projected_points text,
    team_2_win_probability text,
    week_end text,
    week_start text,
    winner_team_key text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_matchup primary key(week, team_1_key, team_2_key, inserted_timestamp)
);
create table if not exists yahoo_data.rosters(
    bye_weeks text,
    editorial_team_abr text,
    editorial_team_full_name text,
    editorial_team_key text,
    editorial_team_url text,
    eligible_positions text[],
    has_player_notes text,
    has_recent_player_notes text,
    injury_note text,
    is_undroppable text,
    player_key text,
    player_notes_last_timestamp text,
    position_type text,
    primary_position text,
    week text constraint week_default default '0',
    selected_position text,
    selected_position_is_flex text,
    team_key text,
    uniform_number text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_roster primary key(team_key, player_key, inserted_timestamp)
);
create table if not exists yahoo_data.draft_results(
    pick text,
    round text,
    league_key text,
    player_key text,
    team_key text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_draft_results primary key(player_key, inserted_timestamp)
);
create table if not exists yahoo_data.player_draft_analysis(
    average_cost text,
    average_pick text,
    average_round text,
    league_key text,
    percent_drafted text,
    player_key text,
    preseason_average_cost text,
    preseason_average_pick text,
    preseason_average_round text,
    preseason_percent_drafted text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_player_draft_analysis primary key(player_key, inserted_timestamp)
);
create table if not exists yahoo_data.player_pct_owned(
    bye_weeks text,
    editorial_player_key text,
    editorial_team_abr text,
    editorial_team_full_name text,
    editorial_team_key text,
    editorial_team_url text,
    first_ascii_name text,
    first_name text,
    full_name text,
    has_player_notes text,
    has_recent_player_notes text,
    headshot_url text,
    injury_note text,
    is_keeper_cost text,
    is_keeper_kept text,
    is_keeper_status text,
    is_undroppable text,
    last_ascii_name text,
    last_name text,
    league_key text,
    week text constraint week_default default '0',
    percent_owned_delta text,
    percent_owned_value text,
    player_id text,
    player_key text,
    player_notes_last_timestamp text,
    player_url text,
    position_type text,
    primary_position text,
    uniform_number text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_player_week_pctowned primary key(week, player_key, inserted_timestamp)
);
create table if not exists yahoo_data.player_stats(
    league_key text,
    player_key text,
    week text constraint week_default default '0',
    stat_id text,
    stat_value text,
    total_points text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_player_week_stat primary key(week, player_key, stat_id, inserted_timestamp)
);
create table if not exists yahoo_data.transactions(
    faab_bid text,
    league_key text,
    player_destination_team_key_1 text,
    player_destination_team_key_2 text,
    player_destination_team_key_3 text,
    player_destination_team_key_4 text,
    player_destination_team_key_5 text,
    player_destination_team_key_6 text,
    player_destination_team_key_7 text,
    player_destination_team_key_8 text,
    player_destination_team_key_9 text,
    player_destination_team_key_10 text,
    player_destination_type_1 text,
    player_destination_type_2 text,
    player_destination_type_3 text,
    player_destination_type_4 text,
    player_destination_type_5 text,
    player_destination_type_6 text,
    player_destination_type_7 text,
    player_destination_type_8 text,
    player_destination_type_9 text,
    player_destination_type_10 text,
    player_key_1 text,
    player_key_2 text,
    player_key_3 text,
    player_key_4 text,
    player_key_5 text,
    player_key_6 text,
    player_key_7 text,
    player_key_8 text,
    player_key_9 text,
    player_key_10 text,
    player_source_type_1 text,
    player_source_type_2 text,
    player_source_type_3 text,
    player_source_type_4 text,
    player_source_type_5 text,
    player_source_type_6 text,
    player_source_type_7 text,
    player_source_type_8 text,
    player_source_type_9 text,
    player_source_type_10 text,
    player_type_1 text,
    player_type_2 text,
    player_type_3 text,
    player_type_4 text,
    player_type_5 text,
    player_type_6 text,
    player_type_7 text,
    player_type_8 text,
    player_type_9 text,
    player_type_10 text,
    tradee_team_key text,
    tradee_team_name text,
    trader_team_key text,
    trader_team_name text,
    transaction_id text,
    transaction_key text,
    status text,
    timestamp text,
    type text,
    inserted_timestamp timestamp without time zone constraint inserted_at_constraint default current_timestamp,
    constraint inserted_at_transaction primary key(transaction_key, inserted_timestamp)
);