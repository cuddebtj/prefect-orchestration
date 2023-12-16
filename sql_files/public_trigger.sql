create trigger _updated_at_time_public_draft_picks_player_metadata
    before update on public.draft_picks_player_metadata
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft_picks
    before update on public.draft_picks
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft_traded_pick
    before update on public.draft_traded_pick
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft_traded_picks
    before update on public.draft_traded_picks
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft_settings
    before update on public.draft_settings
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft_metadata
    before update on public.draft_metadata
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_draft
    before update on public.draft
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_drafts
    before update on public.league_drafts
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_postseason_brackets
    before update on public.league_postseason_brackets
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_losers_bracket
    before update on public.league_losers_bracket
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_winners_bracket
    before update on public.league_winners_bracket
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_matchups_team
    before update on public.league_matchups_team
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_matchups
    before update on public.league_matchups
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_rosters_team
    before update on public.league_rosters_team
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_rosters
    before update on public.league_rosters
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_transaction
    before update on public.league_transaction
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_transactions
    before update on public.league_transactions
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_user_metadata
    before update on public.league_user_metadata
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_user
    before update on public.league_user
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_users
    before update on public.league_users
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_scoring_settings
    before update on public.league_scoring_settings
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_settings
    before update on public.league_settings
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league_metadata
    before update on public.league_metadata
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_league
    before update on public.league
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_player_metadata
    before update on public.player_metadata
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_player_data
    before update on public.player_data
    for each row execute procedure public.update_modified_column();

create trigger _updated_at_time_public_players
    before update on public.players
    for each row execute procedure public.update_modified_column();
