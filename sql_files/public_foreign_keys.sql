alter table public.draft_picks
    add CONSTRAINT draft_picks_player_metadata_fk FOREIGN KEY (player_id) REFERENCES public.draft_picks_player_metadata(player_id);
alter table public.draft_picks
    add CONSTRAINT draft_fk FOREIGN KEY (draft_id) REFERENCES public.draft(draft_id);

alter table public.draft_traded_picks
    add CONSTRAINT draft_fk FOREIGN KEY (draft_id) REFERENCES public.draft(draft_id);

alter table public.draft_settings
    add CONSTRAINT draft_fk FOREIGN KEY (draft_id) REFERENCES public.draft(draft_id);

alter table public.draft_metadata
    add CONSTRAINT draft_fk FOREIGN KEY (draft_id) REFERENCES public.draft(draft_id);

alter table public.league_drafts
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_postseason_brackets
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_losers_bracket
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_winners_bracket
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_matchups_team
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_matchups
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_rosters_team
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_rosters
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_transaction
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_transactions
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_user_metadata
    add CONSTRAINT league_user_fk FOREIGN KEY (user_id) REFERENCES public.league_user(user_id);

alter table public.league_users
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_scoring_settings
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_settings
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league_metadata
    add CONSTRAINT league_fk FOREIGN KEY (league_id) REFERENCES public.league(league_id);

alter table public.league
    add CONSTRAINT league_losers_bracket_fk FOREIGN KEY (loser_bracket_id) REFERENCES public.league_losers_bracket(league_losers_bracket_id);
alter table public.league
    add CONSTRAINT league_winners_bracket_fk FOREIGN KEY (bracket_id) REFERENCES public.league_winners_bracket(league_winners_bracket_id);

alter table public.player_metadata
    add CONSTRAINT player_data_fk FOREIGN KEY (player_id) REFERENCES public.player_data(player_id);

alter table public.players
    add CONSTRAINT player_data_fk FOREIGN KEY (player_id) REFERENCES public.player_data(player_id);
