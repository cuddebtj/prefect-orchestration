import calendar
import json
import logging
import math
import os
from collections import deque, namedtuple
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from io import StringIO
from typing import Any

import psycopg
from dateutil.rrule import MINUTELY, MO, MONTHLY, SA, SU, TH, TU, WEEKLY, rrule
from dotenv import load_dotenv
from polars import DataFrame
from prefect import task
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from psycopg import sql
from pydantic import SecretStr
from pytz import timezone
from yahoo_export import Config, YahooAPI
from yahoo_parser import GameParser, LeagueParser, PlayerParser, TeamParser, YahooParseBase

load_dotenv()

logger = logging.getLogger(__name__)  # type: ignore

NFLWeek = namedtuple("NFLWeek", ["week", "week_start", "week_end"])


@dataclass
class DatabaseParameters:
    __slots__ = ["db_conn_uri", "schema_name", "table_name"]
    db_conn_uri: SecretStr
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
            self.current_timestamp.year if self.current_timestamp.month > 4 else self.current_timestamp.year - 1
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


@lru_cache
def define_pipeline_schedules(current_timestamp: datetime) -> tuple[str, str, str]:
    nfl_season = get_week(current_timestamp, get_all_weeks=True)
    start_date = nfl_season[0].week_start
    end_date = nfl_season[-2].week_end + timedelta(days=1)

    sunday_schedule = rrule(
        freq=MINUTELY,
        dtstart=start_date,
        interval=5,
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
        byweekday=MO,
        bymonth=(5, 9),
    )
    return str(sunday_schedule), str(weekly_schedule), str(off_pre_schedule)


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
        return cal[0][0]
    else:
        return cal[1][0]


END_POINT_TABLE_MAP = {
    "get_all_game_keys_game_key_df": "allgames",
    "get_game_game_df": "games",
    "get_game_game_week_df": "game_weeks",
    "get_game_game_stat_categories_df": "stat_categories",
    "get_game_game_position_type_df": "posisition_types",
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
]  # between june 1st and september 1st
OFFSEASON_END_POINTS = ["get_all_game_keys", "get_league_offseason"]  # between march 1st and june 1st
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
            return NFLWeek(week=(week + 1), week_start=current_week_wednesday, week_end=next_week_tuesday)

    if current_date < nfl_season[0].week_start or current_date > nfl_season[-1].week_end:
        return NFLWeek(week=0, week_start=current_date, week_end=current_date)
    else:
        return nfl_season


def get_data_from_db(connection_str: str, sql_query: sql.Composed) -> list[Any]:
    """
    Copy data from postgres
    """
    conn = psycopg.connect(connection_str)
    logger.info("Connection to postgres database successful.")

    try:
        curs = conn.cursor()
        curs.execute(sql_query)  # type: ignore
        query_results = curs.fetchall()
        logger.info("SQL query executed successfully.")

    except (Exception, psycopg.DatabaseError) as error:  # type: ignore
        logger.exception(f"Error with database:\n\n{error}\n\n")
        conn.rollback()
        raise error

    finally:
        conn.commit()
        conn.close()
        logger.info("Postgres connection closed.")

    return query_results


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=7))
def get_team_key_list(league_key: str, num_teams: int) -> list[str]:
    return [f"{league_key}.t.{team_id}" for team_id in range(1, num_teams + 1)]


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=7))
def get_player_key_list(db_conn_uri: SecretStr, league_key: str) -> list[str]:
    sql_str = "select distinct player_key from yahoo_data.players where league_key = %s"
    sql_query = sql.SQL(sql_str).format(sql.Literal(league_key))
    player_key_list = get_data_from_db(db_conn_uri.get_secret_value(), sql_query)
    return player_key_list


@task
def chunk_list_twenty_five(input_list: list[str]) -> Generator[list[str], None, None]:
    deque_obj = deque(input_list)

    while deque_obj:
        chunk = []
        for _ in range(25):
            if deque_obj:
                chunk.append(deque_obj.popleft())

        yield chunk


@task
def determine_end_points(pipeline_params: PipelineParameters) -> set[str]:
    nfl_season = get_week(pipeline_params.current_timestamp, get_all_weeks=True)
    current_week = pipeline_params.current_week
    nfl_start_date = nfl_season[0].week_start
    nfl_end_date = nfl_season[-1].week_end
    nfl_end_week = nfl_season[-1].week
    current_date = pipeline_params.current_timestamp.astimezone(timezone("America/Denver")).date()  # type: ignore
    current_day_of_week = current_date.weekday()  # type: ignore
    may_first = datetime(current_date.year, 6, 1, tzinfo=timezone("UTC")).astimezone(timezone("America/Denver")).date()

    end_points = []
    # preseason or offseason
    if current_week == OFFSEASON_WEEK:
        # preseason
        if current_date < nfl_start_date and current_date >= may_first:  # type: ignore
            end_points += PRESEASON_END_POINTS
        # offseason
        if current_date < may_first and current_date >= nfl_end_date:  # type: ignore
            end_points += OFFSEASON_END_POINTS
    # regular season -> live or weekly
    if current_week > OFFSEASON_WEEK and current_week < nfl_end_week:
        # get prior week score adjustments and next week matchups
        if current_day_of_week == TUESDAY:
            end_points += BEGINNING_OF_WEEK_END_POINTS
            end_points += LIVE_END_POINTS
        # get player data live
        if current_day_of_week in [THURSDAY, SUNDAY, MONDAY]:
            end_points += LIVE_END_POINTS
        # get player pct owned and roster before Sunday
        if current_day_of_week == SATURDAY:
            end_points += BEFORE_MAIN_SLATE_WEEKLY_END_POINTS
    # following end_points are require looping over all players for full data
    # get_players, get_player_draft_analysis, get_player_stat, get_player_pct_owned

    return set(end_points)


@task
def get_endpoint_config(
    end_point: str,
    page_start: int | None,
    retrieval_limit: int | None,
    player_key_list: list[str] | None,
) -> EndPointParameters:
    end_point_params = EndPointParameters(
        end_point=end_point,
        data_key_list=None,
        start=player_key_list[0] if player_key_list else None,
        end=player_key_list[-1] if player_key_list else None,
        page_start=page_start,
        retrieval_limit=retrieval_limit,
        player_key_list=player_key_list,
    )

    match end_point:
        case "get_all_game_keys":
            end_point_params.data_key_list = ["games"]

        case "get_player":
            end_point_params.page_start = page_start if page_start else 0
            end_point_params.retrieval_limit = retrieval_limit if retrieval_limit else 25

        case "get_player_draft_analysis" | "get_player_stat" | "get_player_pct_owned":
            if not player_key_list:
                error_msg = f"player_key_list must be provided for this end_point: {end_point}"
                raise ValueError(error_msg)

        case _:
            error_msg = f"Invalid end_point: {end_point}"
            raise ValueError(error_msg)

    return end_point_params


@task
def split_pipelines(
    end_point_list: list[EndPointParameters],
) -> tuple[list[EndPointParameters], list[EndPointParameters] | None, list[EndPointParameters] | None]:
    pipeline_length = len(end_point_list)

    if pipeline_length >= 3:  # noqa: PLR2004
        chunk_size = math.ceil(pipeline_length / 3)
        chunk_one = end_point_list[:chunk_size]
        chunk_two = end_point_list[chunk_size : chunk_size * 2]
        chunk_three = end_point_list[chunk_size * 2 :]

    else:
        chunk_one = end_point_list
        chunk_two = None
        chunk_three = None

    return chunk_one, chunk_two, chunk_three


@task
def extractor(
    pipeline_params: PipelineParameters, end_point_params: EndPointParameters, yahoo_api: YahooAPI
) -> tuple[dict[str, str], YahooParseBase]:
    pipeline_args = {
        "game_key": str(pipeline_params.game_id),
        "league_key": pipeline_params.league_key,
        "week": pipeline_params.current_week,
        "team_key_list": pipeline_params.team_key_list,
        "start_count": end_point_params.page_start,
        "retrieval_limit": end_point_params.retrieval_limit,
        "player_key_list": end_point_params.player_key_list,
        "data_key_list": end_point_params.data_key_list,
        "start": end_point_params.start,
        "end": end_point_params.end,
    }

    query_args_list = [
        "game_key",
        "league_key",
        "week",
        "team_key_list",
        "start_count",
        "retrieval_limit",
        "player_key_list",
    ]
    parse_args_list = [
        "game_key",
        "data_key_list",
        "league_key",
        "week",
        "start",
        "end",
    ]

    extract_objects = {
        "get_all_game_keys": (yahoo_api.get_all_game_keys, GameParser),
        "get_game": (yahoo_api.get_game, GameParser),
        "get_league_preseason": (yahoo_api.get_league_preseason, LeagueParser),
        "get_league_draft_result": (yahoo_api.get_league_draft_result, LeagueParser),
        "get_league_matchup": (yahoo_api.get_league_matchup, LeagueParser),
        "get_league_transaction": (yahoo_api.get_league_transaction, LeagueParser),
        "get_league_offseason": (yahoo_api.get_league_offseason, LeagueParser),
        "get_roster": (yahoo_api.get_roster, TeamParser),
        "get_player": (yahoo_api.get_player, PlayerParser),
        "get_player_draft_analysis": (yahoo_api.get_player_draft_analysis, PlayerParser),
        "get_player_stat": (yahoo_api.get_player_stat, PlayerParser),
        "get_player_pct_owned": (yahoo_api.get_player_pct_owned, PlayerParser),
    }

    query_args = {}
    for arg in query_args_list:
        query_args.update({arg: pipeline_args[arg]})

    parse_args = {}
    for arg in parse_args_list:
        query_args.update({arg: pipeline_args[arg]})

    extract_obj = extract_objects[end_point_params.end_point]
    resp, _ = extract_obj[0](**query_args)

    if end_point_params.end_point in ["get_all_game_keys", "get_game", "get_roster"]:
        parser = extract_obj[1](
            response=resp,
            season=pipeline_params.current_season,
            **parse_args,
        )
    else:
        parser = extract_obj[1](
            response=resp,
            season=pipeline_params.current_season,
            end_point=end_point_params.end_point,
            **parse_args,
        )

    return resp, parser


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


@task
def parse_response(data_parser: YahooParseBase, end_point: str) -> dict[str, DataFrame]:
    parsing_methods = get_parsing_methods(end_point, data_parser)

    df_dict = {}
    for parse_name, parse_method in parsing_methods.items():
        mapped_table = END_POINT_TABLE_MAP[f"{end_point}_{parse_name}"]
        df_dict.update({mapped_table: parse_method()})

    return df_dict


@task
def json_to_db(raw_data: dict, db_params: DatabaseParameters, columns: list[str] | None = None) -> None:
    """
    Copy data into postgres
    """
    # schema_name = database_parameters.schema_name
    schema_name = "yahoo_json"
    set_schema_statement = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))

    copy_statement = "COPY {0} ({1}) FROM STDIN"
    column_names = [sql.Identifier(col) for col in columns] if columns else [sql.Identifier("yahoo_json")]
    copy_query = sql.SQL(copy_statement).format(sql.Identifier(db_params.table_name), *column_names)  # type: ignore

    file_buffer = StringIO()  # type: ignore
    json.dump(raw_data, file_buffer)  # type: ignore
    file_buffer.seek(0)

    conn = psycopg.connect(db_params.db_conn_uri.get_secret_value())
    logger.info("Connection to postgres database successful.")

    try:
        curs = conn.cursor()
        curs.execute(set_schema_statement)

        with curs.copy(copy_query) as copy:
            copy.write(file_buffer.read())

        status_msg = curs.statusmessage
        logger.info(f"JSON response copied successfully.\n\t{status_msg}")

    except (Exception, psycopg.DatabaseError) as error:  # type: ignore
        logger.exception(f"Error with database:\n\n{error}\n\n")
        conn.rollback()
        raise error

    finally:
        conn.commit()
        conn.close()
        logger.info("Postgres connection closed.")


@task
def df_to_db(resp_table_df: DataFrame, db_params: DatabaseParameters) -> None:
    schema_name = "yahoo_data"
    table_name = f"{schema_name}.{db_params.table_name}"
    resp_table_df.write_database(
        table_name=table_name,
        connection=db_params.db_conn_uri.get_secret_value(),
        engine="adbc",
    )
    logger.info("Dataframe successfully appended to database.")


@task
def get_yahoo_api_config(how_many_conig: int) -> Config | list[Config]:
    env_status = None  # os.getenv("ENVIRONMENT", "local")

    if how_many_conig == 1:
        consumer_key = SecretStr(
            os.getenv("YAHOO_CONSUMER_KEY_ONE", "key_one")
            if env_status == "local"
            else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
        )
        consumer_secret = SecretStr(
            os.getenv("YAHOO_CONSUMER_SECRET_ONE", "secret_one")
            if env_status == "local"
            else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
        )
        tokey_file_path = "oauth_token_one.yaml"
        config_return = Config(
            yahoo_consumer_key=consumer_key,
            yahoo_consumer_secret=consumer_secret,
            token_file_path=tokey_file_path,
        )

    else:
        num_to_words = {
            1: "one",
            2: "two",
            3: "three",
        }
        config_return = []
        for config_num in range(1, how_many_conig + 1):
            config_num_str = num_to_words[config_num]
            consumer_key = SecretStr(
                os.getenv(f"YAHOO_CONSUMER_KEY_{config_num_str.upper()}", f"key_{config_num_str}")
                if env_status == "local"
                else Secret.load(f"yahoo-consumer-key-{config_num_str}").get()  # type: ignore
            )
            consumer_secret = SecretStr(
                os.getenv(f"YAHOO_CONSUMER_SECRET_{config_num_str.upper()}", f"secret_{config_num_str}")
                if env_status == "local"
                else Secret.load(f"yahoo-consumer-secret-{config_num_str}").get()  # type: ignore
            )
            tokey_file_path = f"oauth_token_{config_num_str}.yaml"
            _config = Config(
                yahoo_consumer_key=consumer_key,
                yahoo_consumer_secret=consumer_secret,
                token_file_path=tokey_file_path,
            )
            config_return.append(_config)

    return config_return
