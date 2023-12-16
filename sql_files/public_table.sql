drop table if exists public.draft_picks_player_metadata;
create table if not exists public.draft_picks_player_metadata (
    years_exp varchar,
    team varchar,
    status varchar,
    sport varchar,
    player_id varchar,
    number varchar,
    news_updated varchar,
    last_name varchar,
    injury_status varchar,
    first_name varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT draft_picks_player_metadata_pk PRIMARY KEY (player_id)
);

drop table if exists public.draft_picks;
create table if not exists public.draft_picks (
    round int,
    roster_id int,
    player_id varchar,
    picked_by varchar,
    pick_no int,
    metadata jsonb,
    is_keeper boolean,
    draft_slot int,
    draft_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT draft_picks_pk PRIMARY KEY (draft_id, pick_no)
);

drop table if exists public.draft_traded_pick;
create table if not exists public.draft_traded_pick (
    season varchar,
    round int,
    roster_id int,
    previous_owner_id int,
    owner_id int,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.draft_traded_picks;
create table if not exists public.draft_traded_picks (
    draft_traded_picks jsonb[],
    draft_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.draft_settings;
create table if not exists public.draft_settings (
    teams int,
    slots_wr int,
    slots_te int,
    slots_rb int,
    slots_qb int,
    slots_flex int,
    slots_def int,
    slots_bn int,
    rounds int,
    reversal_round int,
    player_type int,
    pick_timer int,
    nomination_timer int,
    cpu_autopick int,
    autostart  int,
    autopause_start_time int,
    autopause_end_time int,
    autopause_enabled int,
    alpha_sort int,
    draft_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.draft_metadata;
create table if not exists public.draft_metadata (
    scoring_type varchar,
    name varchar,
    description varchar,
    draft_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.draft;
create table if not exists public.draft (
    type varchar,
    status varchar,
    start_time int,
    sport varchar,
    slot_to_roster_id jsonb,
    settings jsonb,
    season_type varchar,
    season varchar,
    metadata jsonb,
    league_id varchar,
    last_picked int,
    last_message_id varchar,
    draft_order jsonb,
    draft_id varchar,
    creators varchar[],
    created int,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT draft_pk PRIMARY KEY (draft_id)
);
comment on column public.draft.slot_to_roster_id is 'Struct of draft slot to roster id';
comment on column public.draft.settings is 'Struct of draft settings';
comment on column public.draft.metadata is 'Struct of draft metadata';
comment on column public.draft.draft_order is 'Struct of draft user id to draft slot';
comment on column public.draft.creators is 'Array of users who created the draft';

drop table if exists public.league_drafts;
create table if not exists public.league_drafts (
    league_drafts jsonb[],
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_drafts.league_drafts is 'Array of drafts';

drop table if exists public.league_postseason_brackets;
create table if not exists public.league_postseason_brackets (
    t1_from jsonb,
    t2_from jsonb,
    t1 int,
    t2 int,
    w int,
    l int,
    r int,
    m int,
    p int,
    bracket varchar,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_postseason_brackets.t1_from is 'Roster id of winner of matchup number';
comment on column public.league_postseason_brackets.t2_from is 'Roster id of winner of matchup number';
comment on column public.league_postseason_brackets.t1 is 'Roster id';
comment on column public.league_postseason_brackets.t2 is 'Roster id';
comment on column public.league_postseason_brackets.m is 'Matchup number';
comment on column public.league_postseason_brackets.r is 'Round number';
comment on column public.league_postseason_brackets.p is 'Playing for place number';

drop table if exists public.league_losers_bracket;
create table if not exists public.league_losers_bracket (
    league_losers_bracket jsonb[],
    league_id varchar,
    league_losers_bracket_id int PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_losers_bracket.league_losers_bracket is 'Array of league postseason brackets, losers';

drop table if exists public.league_winners_bracket;
create table if not exists public.league_winners_bracket (
    league_winners_bracker jsonb[],
    league_id varchar,
    league_winners_bracket_id int PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_winners_bracket.league_winners_bracker is 'Array of league postseason brackets, winners';

drop table if exists public.league_matchups_team;
create table if not exists public.league_matchups_team (
    player_points jsonb[],
    starter_points numeric[],
    starters varchar[],
    matchup_id int,
    custom_points numeric[],
    roster_id int,
    players varchar[],
    points numeric,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT league_matchups_team_pk PRIMARY KEY (league_id, matchup_id, roster_id)
);
comment on column public.league_matchups_team.player_points is 'Struct of all player ids to player points';
comment on column public.league_matchups_team.starter_points is 'Array of started player points';
comment on column public.league_matchups_team.starters is 'Array of player ids starting';
comment on column public.league_matchups_team.custom_points is 'Array of custom edited player points if commissioner overrides points manually';
comment on column public.league_matchups_team.players is 'Array of all player ids';

drop table if exists public.league_matchups;
create table if not exists public.league_matchups (
    league_matchups jsonb[],
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_matchups.league_matchups is 'Array of league matchups team';

drop table if exists public.league_rosters_team;
create table if not exists public.league_rosters_team (
    taxi int,
    starters varchar[],
    settings jsonb,
    roster_id int,
    reserve int,
    players varchar[],
    player_map varchar,
    owner_id varchar,
    metadata jsonb,
    league_id varchar,
    keepers varchar[],
    co_owners varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT league_rosters_team_pk PRIMARY KEY (league_id, owner_id)
);
comment on column public.league_rosters_team.starters is 'Array of player ids starting';
comment on column public.league_rosters_team.players is 'Array of all player ids';
comment on column public.league_rosters_team.keepers is 'Array of keeper player ids';

drop table if exists public.league_rosters;
create table if not exists public.league_rosters (
    league_rosters jsonb[],
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_rosters.league_rosters is 'Array of league rosters team';

drop table if exists public.league_transaction;
create table if not exists public.league_transaction (
    waiver_budget jsonb,
    status_updated int,
    roster_ids int[],
    drops jsonb,
    consenter_ids int[],
    adds jsonb,
    transaction_id varchar,
    creator varchar,
    draft_picks int[],
    leg int,
    settings jsonb,
    created int,
    metadata jsonb,
    type varchar,
    status varchar,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT league_transaction_pk PRIMARY KEY (transaction_id)
);
comment on column public.league_transaction.waiver_budget is 'Array of waiver budgets traded';
comment on column public.league_transaction.roster_ids is 'Array of roster ids';
comment on column public.league_transaction.drops is 'Struct of drops, player id to roster id';
comment on column public.league_transaction.consenter_ids is 'Array of roster ids approving transaction';
comment on column public.league_transaction.adds is 'Struct of adds, player id to roster id';
comment on column public.league_transaction.draft_picks is 'Array of draft picks traded';
comment on column public.league_transaction.settings is 'Struct of transaction settings';
comment on column public.league_transaction.metadata is 'Struct of transaction metadata';

drop table if exists public.league_transactions;
create table if not exists public.league_transactions (
    league_transactions jsonb[],
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_transactions.league_transactions is 'Array of league transaction';

drop table if exists public.league_user_metadata;
create table if not exists public.league_user_metadata (
    mention_pn varchar,
    allow_sms varchar,
    allow_pn varchar,
    user_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.league_user;
create table if not exists public.league_user (
    user_id varchar,
    username varchar,
    metadata jsonb,
    is_owner boolean,
    is_bot boolean,
    display_name varchar,
    avatar varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT league_user_pk PRIMARY KEY (user_id)
);
comment on column public.league_user.metadata is 'Struct of user metadata';

drop table if exists public.league_users;
create table if not exists public.league_users (
    league_users jsonb[],
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.league_users.league_users is 'Array of league users';

drop table if exists public.league_scoring_settings;
create table if not exists public.league_scoring_settings (
    st_ff numeric,
    pts_allow_7_13 numeric,
    def_st_ff numeric,
    rec_yd numeric,
    fum_rec_td numeric,
    pts_allow_35p numeric,
    pts_allow_28_34 numeric,
    fum numeric,
    rush_yd numeric,
    pass_td numeric,
    blk_kick numeric,
    pass_yd numeric,
    safe numeric,
    def_td numeric,
    fgm_50p numeric,
    def_st_td numeric,
    fum_rec numeric,
    rush_2pt numeric,
    xpm numeric,
    pts_allow_21_27 numeric,
    fgm_20_29 numeric,
    pts_allow_1_6 numeric,
    fum_lost numeric,
    def_st_fum_rec numeric,
    int numeric,
    fgm_0_19 numeric,
    pts_allow_14_20 numeric,
    rec numeric,
    ff numeric,
    fgmiss numeric,
    st_fum_rec numeric,
    rec_2pt numeric,
    rush_td numeric,
    xpmiss numeric,
    fgm_30_39 numeric,
    rec_td numeric,
    st_td numeric,
    pass_2pt numeric,
    pts_allow_0 numeric,
    pass_int numeric,
    fgm_yds numeric,
    fgm_40_49 numeric,
    sack numeric,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.league_settings;
create table if not exists public.league_settings (
    daily_waivers_last_ran int,
    reserve_allow_cov int,
    reserve_slots int,
    leg int,
    offseason_adds int,
    bench_lock int,
    trade_review_days int,
    league_average_match int,
    waiver_type int,
    max_keepers int,
    type int,
    pick_trading int,
    daily_waivers int,
    taxi_years int,
    trade_deadline int,
    reserve_allow_sus int,
    reserve_allow_out int,
    playoff_round_type int,
    waiver_day_of_week int,
    taxi_allow_vets int,
    reserve_allow_dnr int,
    commissioner_direct_invite int,
    reserve_allow_doubtful int,
    waiver_clear_days int,
    playoff_week_start int,
    daily_waivers_days int,
    last_scored_leg int,
    taxi_slots int,
    playoff_type int,
    daily_waivers_hour int,
    num_teams int,
    playoff_teams int,
    playoff_seed_type int,
    start_week int,
    reserve_allow_na int,
    draft_rounds int,
    taxi_deadline int,
    waiver_bid_min int,
    capacity_override int,
    disable_adds int,
    waiver_budget int,
    last_report int,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.league_metadata;
create table if not exists public.league_metadata (
    keeper_deadline varchar,
    league_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.league;
create table if not exists public.league (
    total_rosters int,
    loser_bracket_id int,
    group_id varchar,
    bracket_id int,
    roster_positions varchar[],
    previous_league_id varchar,
    league_id varchar,
    last_read_id varchar,
    last_pinned_message_id varchar,
    last_message_time int,
    last_message_text_map varchar,
    last_message_attachment varchar,
    last_author_is_bot boolean,
    draft_id varchar,
    last_author_id varchar,
    last_author_display_name varchar,
    last_author_avatar varchar,
    last_message_id varchar,
    avatar varchar,
    shard int,
    sport varchar,
    season_type varchar,
    season varchar,
    scoring_settings jsonb,
    company_id varchar,
    settings jsonb,
    metadata jsonb,
    status varchar,
    name varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT league_pk PRIMARY KEY (league_id)
);
comment on column public.league.roster_positions is 'Array of roster positions';
comment on column public.league.scoring_settings is 'Struct of league scoring settings';
comment on column public.league.settings is 'Struct of league settings';
comment on column public.league.metadata is 'Struct of league metadata';

drop table if exists public.player_metadata;
create table if not exists public.player_metadata (
    rookie_year varchar,
    player_id varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);

drop table if exists public.player_data;
create table if not exists public.player_data (
    college varchar,
    weight varchar,
    practice_description varchar,
    espn_id bigint,
    team varchar,
    sportradar_id varchar,
    full_name varchar,
    injury_status varchar,
    injury_start_date varchar,
    position varchar,
    birth_state varchar,
    rotowire_id bigint,
    swish_id bigint,
    player_id varchar,
    metadata jsonb,
    oddsjam_id varchar,
    number int,
    injury_notes varchar,
    search_first_name varchar,
    hashtag varchar,
    fantasy_positions varchar[],
    last_name varchar,
    injury_body_part varchar,
    birth_city varchar,
    practice_participation varchar,
    age int,
    birth_country varchar,
    stats_id bigint,
    gsis_id varchar,
    pandascore_id varchar,
    status varchar,
    search_last_name varchar,
    high_school varchar,
    sport varchar,
    rotoworld_id int,
    news_updated bigint,
    birth_date varchar,
    first_name varchar,
    active boolean,
    fantasy_data_id int,
    depth_chart_order int,
    years_exp int,
    height varchar,
    search_full_name varchar,
    yahoo_id bigint,
    search_rank bigint,
    depth_chart_position varchar,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW(),
    CONSTRAINT player_pk PRIMARY KEY (player_id)
);
comment on column public.player_data.fantasy_positions is 'Array of fantasy positions';

drop table if exists public.players;
create table if not exists public.players (
    player_id varchar,
    player_data jsonb,
    modified_timestamp timestamp CONSTRAINT modified_at_constraint DEFAULT NOW()
);
comment on column public.players.player_data is 'Struct of player data';

drop table if exists public.nfl_state;
create table if not exists public.nfl_state (
    season_start_date date,
    previous_season varchar,
    league_create_season varchar,
    display_week int,
    league_season varchar,
    season_type varchar,
    season varchar,
    leg int,
    week int
);

