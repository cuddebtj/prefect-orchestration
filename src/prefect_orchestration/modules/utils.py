import calendar
import logging
from collections import deque, namedtuple
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any

import psycopg
from dateutil.rrule import MINUTELY, MO, MONTHLY, SA, SU, TH, TU, WEEKLY, rrule
from psycopg import Connection, sql
from pytz import timezone
from yahoo_parser import YahooParseBase

NFLWeek = namedtuple("NFLWeek", ["week", "week_start", "week_end"])

logger = logging.getLogger(__name__)


@dataclass
class DatabaseParameters:
    __slots__ = ["db_conn", "schema_name", "table_name"]
    db_conn: Connection
    schema_name: str | None
    table_name: str | None


@dataclass
class PipelineParameters:
    __slots__ = [
        "current_timestamp",
        "game_id",
        "league_key",
        "num_of_teams",
        "current_season",
        "current_week",
        "team_key_list",
    ]
    current_timestamp: datetime
    game_id: int | str
    league_key: str
    num_of_teams: int

    def __post_init__(self):
        self.current_season = (
            self.current_timestamp.year if self.current_timestamp.month > 1 else self.current_timestamp.year - 1
        )
        self.current_week = get_week(self.current_timestamp).week  # type: ignore
        self.team_key_list = get_team_key_list(self.league_key, num_teams=self.num_of_teams)


@dataclass
class EndPointParameters:
    __slots__ = [
        "end_point",
        "data_key_list",
        "start",
        "end",
        "page_start",
        "retrieval_limit",
        "player_key_list",
    ]
    end_point: str
    data_key_list: list[str] | None
    start: str | None
    end: str | None
    page_start: int | None
    retrieval_limit: int | None
    player_key_list: list[str] | None


def chunk_to_twentyfive_items(input_list: list[str]) -> list[list[str]]:
    deque_obj = deque(input_list)

    chunks = []
    while deque_obj:
        chunk = []
        for _ in range(25):
            if deque_obj:
                chunk.append(deque_obj.popleft())

        chunks.append(chunk)

        # yield chunk
    return chunks


def get_data_from_db(db_conn: Connection, sql_query: sql.Composed) -> list[Any]:
    """
    Copy data from postgres
    """

    try:
        curs = db_conn.cursor()
        curs.execute(sql_query)  # type: ignore
        query_results = curs.fetchall()
        logger.info(f"SQL query executed successfully:\n\t{sql_query}")

    except (Exception, psycopg.DatabaseError) as error:  # type: ignore
        logger.exception(f"Error with database:\n\n{error}\n\n")
        db_conn.rollback()
        logger.info("Postgres transaction rolled back.")
        raise error

    else:
        logger.info(f"Row counts returend: {len(query_results)}.")
        return query_results

    finally:
        db_conn.commit()
        logger.info("Postgres transaction commited.")


@lru_cache
def define_pipeline_schedules(current_timestamp: datetime) -> tuple[str, str, str]:
    nfl_season = get_week(current_timestamp, get_all_weeks=True)
    start_date = nfl_season[0].week_start
    end_date = nfl_season[-2].week_end + timedelta(days=1)

    sunday_schedule = rrule(
        freq=MINUTELY,
        dtstart=start_date,
        interval=10,
        until=end_date,
        byweekday=SU,
        byhour=range(6, 23),
    )
    weekly_schedule = rrule(
        freq=WEEKLY,
        dtstart=start_date,
        interval=1,
        until=end_date + timedelta(days=1),
        byweekday=(MO, TU, TH, SA),
        byhour=(11, 17),
    )
    off_pre_schedule = rrule(
        freq=MONTHLY,
        dtstart=datetime(current_timestamp.year, 1, 1),  # noqa: DTZ001
        interval=1,
        until=datetime(current_timestamp.year, 12, 31),  # noqa: DTZ001
        bysetpos=1,
        byweekday=TU,
        bymonth=(5, 9),
    )
    return str(sunday_schedule), str(weekly_schedule), str(off_pre_schedule)


@lru_cache
def get_team_key_list(league_key: str, num_teams: int) -> list[str]:
    return [f"{league_key}.t.{team_id}" for team_id in range(1, num_teams + 1)]


@lru_cache
def get_labor_day(current_timestamp: date) -> date:
    """
    Calculates when Labor day is of the given year
    """
    year = current_timestamp.year
    september = 9
    if current_timestamp < datetime(year, 3, 1, tzinfo=timezone("America/Denver")).date():
        year -= 1
    mycal = calendar.Calendar(0)
    cal = mycal.monthdatescalendar(year, september)
    if cal[0][0].month == september:
        logger.info(f"Labor Day is {cal[0][0]}")
        return cal[0][0]
    else:
        logger.info(f"Labor Day is {cal[0][0]}")
        return cal[1][0]


@lru_cache
def get_week(
    current_timestamp: datetime, get_all_weeks: bool = False  # noqa: FBT001, FBT002
) -> NFLWeek | list[NFLWeek]:
    current_date = current_timestamp.astimezone(timezone("America/Denver")).date()
    labor_day = get_labor_day(current_date)
    days_to_current_wednesday = 2
    days_to_next_tuesday = 8

    nfl_season = []
    for week in range(0, 18):
        current_week_wednesday = labor_day + timedelta(days=((week * 7) + days_to_current_wednesday))
        next_week_tuesday = labor_day + timedelta(days=((week * 7) + days_to_next_tuesday))
        nfl_week = NFLWeek(week=(week + 1), week_start=current_week_wednesday, week_end=next_week_tuesday)
        nfl_season.append(nfl_week)

        if current_date >= current_week_wednesday and current_date < next_week_tuesday and get_all_weeks is False:
            nfl_week = NFLWeek(week=(week + 1), week_start=current_week_wednesday, week_end=next_week_tuesday)
            logger.info(f"NFL Week {nfl_week}")
            return nfl_week

    if get_all_weeks is True:
        logger.info("NFL Season returned.")
        return nfl_season
    else:
        nfl_week = NFLWeek(week=0, week_start=current_date, week_end=current_date)
        logger.info(f"NFL Week {nfl_week}")
        return NFLWeek(week=0, week_start=current_date, week_end=current_date)


@lru_cache
def get_parsing_methods(end_point: str, data_parser: YahooParseBase) -> dict[str, Callable]:
    match end_point:
        case "get_all_game_keys":
            return {"game_key_df": data_parser.game_key_df}  # type: ignore

        case "get_game":
            return {
                "game_df": data_parser.game_df,  # type: ignore
                "game_week_df": data_parser.game_week_df,  # type: ignore
                "game_stat_categories_df": data_parser.game_stat_categories_df,  # type: ignore
                "game_position_type_df": data_parser.game_position_type_df,  # type: ignore
                "game_roster_positions_df": data_parser.game_roster_positions_df,  # type: ignore
            }

        case "get_league_preseason":
            return {
                "league_df": data_parser.league_df,  # type: ignore
                "team_df": data_parser.team_df,  # type: ignore
                "setting_df": data_parser.setting_df,  # type: ignore
                "roster_position_df": data_parser.roster_position_df,  # type: ignore
                "stat_category_df": data_parser.stat_category_df,  # type: ignore
                "stat_group_df": data_parser.stat_group_df,  # type: ignore
                "stat_modifier_df": data_parser.stat_modifier_df,  # type: ignore
            }

        case "get_league_draft_result":
            return {
                "league_df": data_parser.league_df,  # type: ignore
                "draft_results_df": data_parser.draft_results_df,  # type: ignore
                "team_df": data_parser.team_df,  # type: ignore
            }

        case "get_league_matchup":
            return {
                "league_df": data_parser.league_df,  # type: ignore
                "matchup_df": data_parser.matchup_df,  # type: ignore
            }

        case "get_league_transaction":
            return {
                "league_df": data_parser.league_df,  # type: ignore
                "transaction_df": data_parser.transaction_df,  # type: ignore
            }

        case "get_league_offseason":
            return {
                "league_df": data_parser.league_df,  # type: ignore
                "draft_results_df": data_parser.draft_results_df,  # type: ignore
                "team_df": data_parser.team_df,  # type: ignore
                "transaction_df": data_parser.transaction_df,  # type: ignore
                "setting_df": data_parser.setting_df,  # type: ignore
                "roster_position_df": data_parser.roster_position_df,  # type: ignore
                "stat_category_df": data_parser.stat_category_df,  # type: ignore
                "stat_group_df": data_parser.stat_group_df,  # type: ignore
                "stat_modifier_df": data_parser.stat_modifier_df,  # type: ignore
            }

        case "get_roster":
            return {
                "team_df": data_parser.team_df,  # type: ignore
                "roster_df": data_parser.roster_df,  # type: ignore
            }

        case "get_player":
            return {
                "player_df": data_parser.player_df,  # type: ignore
            }

        case "get_player_draft_analysis":
            return {
                "player_df": data_parser.player_df,  # type: ignore
                "draft_analysis_df": data_parser.draft_analysis_df,  # type: ignore
            }

        case "get_player_stat":
            return {
                "player_df": data_parser.player_df,  # type: ignore
                "stats_df": data_parser.stats_df,  # type: ignore
            }

        case "get_player_pct_owned":
            return {
                "player_df": data_parser.player_df,  # type: ignore
                "pct_owned_meta_df": data_parser.pct_owned_meta_df,  # type: ignore
            }

        case _:
            error_msg = f"Invalid end_point: {end_point}"
            raise ValueError(error_msg)


END_POINT_TABLE_MAP = {
    "get_all_game_keys_game_key_df": "allgames",
    "get_game_game_df": "games",
    "get_game_game_week_df": "game_weeks",
    "get_game_game_stat_categories_df": "stat_categories",
    "get_game_game_position_type_df": "position_types",
    "get_game_game_roster_positions_df": "roster_positions",
    "get_league_preseason_league_df": "leagues",
    "get_league_preseason_team_df": "teams",
    "get_league_preseason_setting_df": "settings",
    "get_league_preseason_roster_position_df": "roster_positions",
    "get_league_preseason_stat_category_df": "stat_categories",
    "get_league_preseason_stat_group_df": "stat_groups",
    "get_league_preseason_stat_modifier_df": "stat_modifiers",
    "get_league_draft_result_league_df": "leagues",
    "get_league_draft_result_draft_results_df": "draft_results",
    "get_league_draft_result_team_df": "teams",
    "get_league_matchup_league_df": "leagues",
    "get_league_matchup_matchup_df": "matchups",
    "get_league_transaction_league_df": "leagues",
    "get_league_transaction_transaction_df": "transactions",
    "get_league_offseason_league_df": "leagues",
    "get_league_offseason_draft_results_df": "draft_results",
    "get_league_offseason_team_df": "teams",
    "get_league_offseason_transaction_df": "transactions",
    "get_league_offseason_setting_df": "settings",
    "get_league_offseason_roster_position_df": "roster_positions",
    "get_league_offseason_stat_category_df": "stat_categories",
    "get_league_offseason_stat_group_df": "stat_groups",
    "get_league_offseason_stat_modifier_df": "stat_modifiers",
    "get_roster_team_df": "teams",
    "get_roster_roster_df": "rosters",
    "get_player_player_df": "players",
    "get_player_draft_analysis_player_df": "players",
    "get_player_draft_analysis_draft_analysis_df": "player_draft_analysis",
    "get_player_stat_player_df": "players",
    "get_player_stat_stats_df": "player_stats",
    "get_player_pct_owned_player_df": "players",
    "get_player_pct_owned_pct_owned_meta_df": "player_pct_owned",
}

PRESEASON_END_POINTS = [
    "get_game",
    "get_league_preseason",
    "get_league_draft_result",
    "get_player",
    "get_player_draft_analysis",
]  # between Labor Day and the first game
OFFSEASON_END_POINTS = [
    "get_all_game_keys",
    "get_league_offseason",
    "get_player",
]  # between march 1st and Labor Day
BEGINNING_OF_WEEK_END_POINTS = ["get_league_matchup"]  # after the monday night game or the tuesday morning after
BEFORE_MAIN_SLATE_WEEKLY_END_POINTS = [
    "get_player_pct_owned",
    "get_roster",
]  # before kickoff of first slate of games, so saturday night
LIVE_END_POINTS = [
    "get_roster",
    "get_player_stat",
]  # while games are being played, #TODO: only for rosterd players?

MONDAY = 0
TUESDAY = 1
THURSDAY = 3
FRIDAY = 4
SATURDAY = 5
SUNDAY = 6
OFFSEASON_WEEK = 0
