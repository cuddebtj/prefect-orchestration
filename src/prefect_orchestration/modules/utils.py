import json
import logging
import math
import os
from collections import deque
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from io import StringIO
from typing import Any

import psycopg
from dotenv import load_dotenv
from polars import DataFrame
from prefect import task
from prefect.tasks import task_input_hash
from psycopg import sql
from pydantic import SecretStr
from pytz import timezone
from yahoo_parser import GameParser, LeagueParser, PlayerParser, TeamParser

from prefect_orchestration.modules.prefect_blocks import YahooConfigBlock, upload_file_to_bucket

load_dotenv()

logger = logging.getLogger(__name__)  # type: ignore

PARSE_CLASS = GameParser | LeagueParser | TeamParser | PlayerParser


@dataclass
class DatabaseParameters:
    __slots__ = ["db_conn_uri", "schema_name", "table_name"]
    db_conn_uri: str
    schema_name: str
    table_name: str | None


@dataclass
class PipelineParameters:
    __slots__ = [
        "yahoo_export_config",
        "db_params",
        "num_of_teams",
        "current_date",
        "game_id",
        "league_key",
        "current_season",
        "current_week",
        "team_key_list",
    ]
    yahoo_export_config: YahooConfigBlock
    db_params: DatabaseParameters
    num_of_teams: int | None
    current_date: datetime | None

    def __post_init__(self):
        self.current_date = self.current_date if self.current_date else datetime.now(tz=timezone("UTC"))
        self.game_id = self.game_id if self.game_id else self.yahoo_export_config.league_info["game_id"]
        self.league_key = (
            self.league_key
            if self.league_key
            else f'{self.game_id}.l.{self.yahoo_export_config.league_info["league_id"]}'
        )
        self.current_season = (
            self.current_season if self.current_season else self.yahoo_export_config.league_info["season"]
        )
        self.current_week = (
            self.current_week
            if self.current_week
            else get_week(
                self.db_params.db_conn_uri,  # type: ignore
                self.game_id,
                self.current_date.date(),
                self.db_params.db_conn_uri.schema_name,  # type: ignore
            )
        )
        self.team_key_list = (
            self.team_key_list
            if self.team_key_list
            else get_team_key_list(self.league_key, num_teams=self.num_of_teams)  # type: ignore
        )


@dataclass
class PipelineConfiguration:
    __slots__ = [
        "pipeline_params",
        "end_point",
        "data_key_list",
        "start",
        "end",
        "page_start",
        "retrieval_limit",
        "player_key_list",
    ]
    pipeline_params: PipelineParameters
    end_point: str
    data_key_list: list[str] | None
    start: str | None
    end: str | None
    page_start: int | None
    retrieval_limit: int | None
    player_key_list: list[str] | None


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


@lru_cache
def get_data_from_db(connection_str: str, sql_query: sql.SQL, schema_name: str) -> list[Any]:
    """
    Copy data from postgres
    """
    conn = psycopg.connect(connection_str)

    logger.info("Connection to postgres database successful.")

    try:
        curs = conn.cursor()

        if schema_name != "":
            sql_search = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))
            curs.execute(sql_search)

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
def get_player_key_list(pipeline_params: PipelineParameters) -> list[str]:
    sql_str = "select distinct player_key from yahoo_data.players where league_key = %s"
    sql_query = sql.SQL(sql_str).format(sql.Literal(pipeline_params.league_key))
    player_key_list = get_data_from_db(
        pipeline_params.db_params.db_conn_uri, sql_query, pipeline_params.db_params.schema_name  # type: ignore
    )
    return player_key_list


@task
def get_week(conn_str: str, game_id: int | str, _date: date, schema_name: str) -> int:
    sql_str = (
        "select distinct game_week from yahoo_data.game_weeks "
        "where game_id = %s and %s::date between game_week_start::date and game_week_end::date;"
    )
    sql_query = sql.SQL(sql_str).format(sql.Literal(game_id), sql.Literal(_date))
    current_week = get_data_from_db(conn_str, sql_query, schema_name)  # type: ignore
    current_week = 0 if not current_week else current_week[0]
    return int(current_week)


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
def get_parameters(
    current_date: datetime | None = None,
    consumer_key: SecretStr | None = None,
    consumer_secret: SecretStr | None = None,
    db_conn_uri: str | None = None,
    num_of_teams: int | None = None,
    season: int = 2023,
    game_id: int = 423,
    league_id: int = 127732,
    schema_name: str = "yahoo_data",
    table_name: str = "test",
) -> PipelineParameters:
    current_date = current_date if current_date else datetime.now(timezone("UTC"))
    consumer_key = consumer_key if consumer_key else SecretStr(os.getenv("YAHOO_CONSUMER_KEY"))  # type: ignore
    consumer_secret = consumer_secret if consumer_secret else SecretStr(os.getenv("YAHOO_CONSUMER_SECRET"))  # type: ignore
    db_conn_uri = db_conn_uri if db_conn_uri else os.getenv("LOCAL_POSTGRES_CONN")  # type: ignore
    num_of_teams = num_of_teams if num_of_teams else 10

    db_conn_params = DatabaseParameters(
        db_conn_uri=db_conn_uri,  # type: ignore
        schema_name=schema_name,
        table_name=table_name,
    )

    yahoo_export_config = YahooConfigBlock(
        consumer_key=consumer_key,  # type: ignore
        consumer_secret=consumer_secret,  # type: ignore
        current_nfl_week=0,
        current_nfl_season=season,
        league_info={"season": season, "game_id": game_id, "league_id": league_id},
    )

    pipeline_params = PipelineParameters(
        yahoo_export_config=yahoo_export_config,
        db_params=db_conn_params,
        num_of_teams=num_of_teams,
        current_date=current_date,
    )

    return pipeline_params


@task
def determine_end_points(pipeline_params: PipelineParameters) -> set[str]:
    current_week = pipeline_params.current_week  # type: ignore
    current_date = pipeline_params.current_date.date()  # type: ignore
    preseason_end_points = [
        "get_game",
        "get_league_preseason",
        "get_league_draft_result",
        "get_player",
        "get_player_draft_analysis",
    ]  # between june 1st and september 1st
    offseason_end_points = ["get_all_game_keys", "get_league_offseason"]  # between march 1st and june 1st
    beginning_of_week_end_points = ["get_league_matchup"]  # after the monday night game or the tuesday morning after
    before_main_slate_weekly_end_points = [
        "get_player_pct_owned",
        "get_roster",
    ]  # before kickoff of first slate of games, so saturday night
    live_end_points = [
        "get_roster",
        "get_player_stat",
    ]  # while games are being played, #TODO: only for rosterd players?

    day_of_week = current_date.weekday()  # type: ignore
    MONDAY = 0  # noqa: N806
    TUESDAY = 1  # noqa: N806
    THURSDAY = 3  # noqa: N806
    FRIDAY = 4  # noqa: N806
    SATURDAY = 5  # noqa: N806
    SUNDAY = 6  # noqa: N806
    SEPTEMBER_FIRST = datetime(current_date.year, 9, 1, tzinfo=timezone("UTC")).date()  # noqa: N806 # type: ignore
    JUNE_FIRST = datetime(current_date.year, 6, 1, tzinfo=timezone("UTC")).date()  # noqa: N806 # type: ignore
    MARCH_FIRST = datetime(current_date.year, 3, 1, tzinfo=timezone("UTC")).date()  # noqa: N806 # type: ignore
    OFFSEASON_WEEK = 0  # noqa: N806
    LAST_REGULAR_SEASON_WEEK = 15  # noqa: N806
    LAST_WEEK = 18  # noqa: N806

    end_points = []

    # preseason or offseason
    if current_week == OFFSEASON_WEEK:
        if current_date < SEPTEMBER_FIRST and current_date >= JUNE_FIRST:  # type: ignore
            end_points += preseason_end_points
        if current_date < JUNE_FIRST and current_date >= MARCH_FIRST:  # type: ignore
            end_points += offseason_end_points

    # regular season -> live or weekly
    if current_week > OFFSEASON_WEEK:
        if current_week < LAST_WEEK:
            if day_of_week == TUESDAY:
                end_points += beginning_of_week_end_points

            if day_of_week in [MONDAY, THURSDAY, SUNDAY]:
                end_points += live_end_points

        if current_week < LAST_REGULAR_SEASON_WEEK and day_of_week == SATURDAY:
            end_points += before_main_slate_weekly_end_points

    # postseason -> live or weekly
    if current_week > LAST_REGULAR_SEASON_WEEK:
        if current_week < LAST_WEEK:
            if day_of_week == FRIDAY:
                end_points += before_main_slate_weekly_end_points

            if day_of_week == SATURDAY:
                end_points += live_end_points

    # following end_points are require looping over all players for full data
    # get_players, get_player_draft_analysis, get_player_stat, get_player_pct_owned

    return set(end_points)


@task
def get_pipeline_config(
    pipeline_params: PipelineParameters,
    end_point: str,
    page_start: int | None,
    retrieval_limit: int | None,
    player_key_list: list[str] | None,
) -> PipelineConfiguration:
    pipeline_config = PipelineConfiguration(
        pipeline_params=pipeline_params,
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
            pipeline_config.data_key_list = ["games"]

        case "get_player":
            pipeline_config.page_start = page_start if page_start else 0
            pipeline_config.retrieval_limit = retrieval_limit if retrieval_limit else 25

        case "get_player_draft_analysis" | "get_player_stat" | "get_player_pct_owned":
            if not player_key_list:
                error_msg = f"player_key_list must be provided for this end_point: {end_point}"
                raise ValueError(error_msg)

        case _:
            error_msg = f"Invalid end_point: {end_point}"
            raise ValueError(error_msg)

    return pipeline_config


@task
def split_pipelines(
    pipe_config_list: list[PipelineConfiguration],
) -> tuple[list[PipelineConfiguration], list[PipelineConfiguration] | None, list[PipelineConfiguration] | None]:
    pipeline_length = len(pipe_config_list)

    if pipeline_length >= 3:  # noqa: PLR2004
        chunk_size = math.ceil(pipeline_length / 3)
        chunk_one = pipe_config_list[:chunk_size]
        chunk_two = pipe_config_list[chunk_size : chunk_size * 2]
        chunk_three = pipe_config_list[chunk_size * 2 :]

    else:
        chunk_one = pipe_config_list
        chunk_two = None
        chunk_three = None

    return chunk_one, chunk_two, chunk_three


@task
def extractor(pipeline_config: PipelineConfiguration) -> tuple[dict[str, str], str, PARSE_CLASS]:
    pipeline_args = {
        "game_key": str(pipeline_config.pipeline_params.game_id),
        "league_key": pipeline_config.pipeline_params.league_key,
        "week": pipeline_config.pipeline_params.current_week,
        "team_key_list": pipeline_config.pipeline_params.team_key_list,
        "start_count": pipeline_config.page_start,
        "retrieval_limit": pipeline_config.retrieval_limit,
        "player_key_list": pipeline_config.player_key_list,
        "data_key_list": pipeline_config.data_key_list,
        "start": pipeline_config.start,
        "end": pipeline_config.end,
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

    yahoo_api = pipeline_config.pipeline_params.yahoo_export_config.get_yahoo_session()
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

    extract_obj = extract_objects[pipeline_config.end_point]
    resp, query_ts = extract_obj[0](**query_args)

    if pipeline_config.end_point in ["get_all_game_keys", "get_game", "get_roster"]:
        parser = extract_obj[1](
            response=resp,
            query_timestamp=query_ts,
            season=pipeline_config.pipeline_params.current_season,
            **parse_args,
        )
    else:
        parser = extract_obj[1](
            response=resp,
            query_timestamp=query_ts,
            season=pipeline_config.pipeline_params.current_season,
            end_point=pipeline_config.end_point,
            **parse_args,
        )

    upload_file_to_bucket(pipeline_config.pipeline_params.yahoo_export_config.token_file_path)

    return resp, query_ts, parser


def get_parsing_methods(end_point: str, data_parser: PARSE_CLASS) -> dict[str, Callable]:
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
def parse_response(data_parser: PARSE_CLASS, end_point: str) -> dict[str, DataFrame]:
    parsing_methods = get_parsing_methods(end_point, data_parser)

    df_dict = {}
    for parse_name, parse_method in parsing_methods.items():
        mapped_table = END_POINT_TABLE_MAP[f"{end_point}_{parse_name}"]
        df_dict.update({mapped_table: parse_method()})

    return df_dict


@task
def json_to_db(data: dict, params_dict: PipelineParameters, columns: list[str]) -> None:
    """
    Copy data into postgres
    """
    file_buffer = StringIO()  # type: ignore
    json.dump(data, file_buffer)  # type: ignore
    file_buffer.seek(0)

    conn = psycopg.connect(params_dict.db_params.db_conn_uri)
    # schema_name = params_dict.db_params.schema_name
    schema_name = "yahoo_data"

    logger.info("Connection to postgres database successful.")

    try:
        curs = conn.cursor()

        sql_search = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))
        curs.execute(sql_search)

        # if columns:
        copy_str = "COPY {0} ({1}) FROM STDIN"
        # column_names = [sql.Identifier(col) for col in columns] if columns else ""
        column_names = [sql.Identifier("yahoo_json")]
        copy_query = sql.SQL(copy_str).format(sql.Identifier(params_dict.db_params.table_name), *column_names)  # type: ignore
        # else:
        #     copy_str = "COPY {0} FROM STDIN"
        #     copy_query = sql.SQL(copy_str).format(sql.Identifier(params_dict.db_params.table_name))  # type: ignore

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
def df_to_db(data_df: DataFrame, params_dict: PipelineParameters) -> None:
    # table_name = (
    #     f"{params_dict.db_params.schema_name}.{params_dict.db_params.table_name}"
    #     if params_dict.db_params.schema_name
    #     else params_dict.db_params.table_name
    # )
    table_name = f"yahoo_data.{params_dict.db_params.table_name}"
    data_df.write_database(table_name=table_name, connection=params_dict.db_params.db_conn_uri, engine="adbc")  # type: ignore
    logger.info("Dataframe successfully appended to database.")
