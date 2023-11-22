import io
import json
import math
import os
from datetime import datetime

import psycopg
from polars import DataFrame
from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from psycopg import sql
from pydantic import SecretStr
from pytz import timezone
from yahoo_export import Config, YahooAPI
from yahoo_parser import GameParser, LeagueParser, PlayerParser, TeamParser, YahooParseBase

from prefect_orchestration.modules.utils import (
    BEFORE_MAIN_SLATE_WEEKLY_END_POINTS,
    BEGINNING_OF_WEEK_END_POINTS,
    END_POINT_TABLE_MAP,
    LIVE_END_POINTS,
    MONDAY,
    OFFSEASON_END_POINTS,
    OFFSEASON_WEEK,
    PRESEASON_END_POINTS,
    SATURDAY,
    SUNDAY,
    THURSDAY,
    TUESDAY,
    DatabaseParameters,
    EndPointParameters,
    PipelineParameters,
    get_parsing_methods,
    get_week,
)


@task
def determine_end_points(pipeline_params: PipelineParameters) -> set[str]:
    logger = get_run_logger()
    nfl_season = get_week(pipeline_params.current_timestamp, get_all_weeks=True)
    current_week = pipeline_params.current_week
    nfl_start_date = nfl_season[0].week_start
    nfl_end_week = nfl_season[-1].week
    current_date = pipeline_params.current_timestamp.astimezone(timezone("America/Denver")).date()  # type: ignore
    current_day_of_week = current_date.weekday()  # type: ignore
    may_first = datetime(current_date.year, 5, 1, tzinfo=timezone("UTC")).astimezone(timezone("America/Denver")).date()
    prior_nfl_end_date = (
        datetime(current_date.year, 1, 1, tzinfo=timezone("UTC")).astimezone(timezone("America/Denver")).date()
    )

    end_points = []
    # preseason or offseason
    if current_week == OFFSEASON_WEEK:
        # preseason
        if current_date < nfl_start_date and current_date >= may_first:  # type: ignore
            end_points += PRESEASON_END_POINTS
        # offseason
        if current_date < may_first and current_date >= prior_nfl_end_date:  # type: ignore
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

    logger_endpoints = "\n\t".join(end_points)
    logger.info(f"Returning player key's:\n\t{logger_endpoints}")
    return set(end_points)


@task
def get_endpoint_config(
    end_point: str,
    page_start: int | None,
    retrieval_limit: int | None,
    player_key_list: list[str] | None,
) -> EndPointParameters:
    logger = get_run_logger()
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

    logger.info(f"Returning end_point configuration for end_point {end_point}.")
    return end_point_params


@task
def split_pipelines(
    end_point_list: list[EndPointParameters],
) -> tuple[list[EndPointParameters], list[EndPointParameters] | None, list[EndPointParameters] | None]:
    logger = get_run_logger()
    pipeline_length = len(end_point_list)
    logger.info(f"Number of pipelines to be run \n\n{pipeline_length!s}\n\n")

    if pipeline_length >= 3:  # noqa: PLR2004
        chunk_size = math.ceil(pipeline_length / 3)
        logger.info(f"Pipeline chunk sizes \n\n{chunk_size!s} \n\n")
        chunk_one = end_point_list[:chunk_size]
        logger.info(f"Pipelines chunk_one size \n\n{len(chunk_one)!s}\n\n")  # type: ignore
        chunk_two = end_point_list[chunk_size : chunk_size * 2]
        logger.info(f"Pipelines chunk_two size \n\n{len(chunk_two)!s}\n\n")  # type: ignore
        chunk_three = end_point_list[chunk_size * 2 :]
        logger.info(f"Pipelines chunk_three size \n\n{len(chunk_three)!s}\n\n")  # type: ignore

        # if (chunk_size * 3) > pipeline_length:
        #     chunk_one = (
        #         chunk_one
        #         if len(chunk_one) == chunk_size
        #         else chunk_one.extend([None for _ in range(chunk_size - len(chunk_one))])  # type: ignore
        #     )
        #     chunk_two = (
        #         chunk_two
        #         if len(chunk_two) == chunk_size
        #         else chunk_two.extend([None for _ in range(chunk_size - len(chunk_two))])  # type: ignore
        #     )
        #     chunk_three = (
        #         chunk_three
        #         if len(chunk_three) == chunk_size
        #         else chunk_three.extend([None for _ in range(chunk_size - len(chunk_three))])  # type: ignore
        #     )
    else:
        chunk_one = end_point_list
        chunk_two = None
        chunk_three = None

    return chunk_one, chunk_two, chunk_three  # type: ignore


@task
def extractor(
    pipeline_params: PipelineParameters, end_point_params: EndPointParameters, yahoo_api: YahooAPI
) -> tuple[dict[str, str], YahooParseBase] | None:
    logger = get_run_logger()
    logger.info(f"Extracting {end_point_params.end_point}")
    if end_point_params.end_point == "get_all_game_keys":
        resp, _ = yahoo_api.get_all_game_keys()
        parser = GameParser(
            response=resp,
            season=pipeline_params.current_season,
            game_key=str(pipeline_params.game_id),
            data_key_list=end_point_params.data_key_list,
        )
        return resp, parser

    elif end_point_params.end_point == "get_game":
        resp, _ = yahoo_api.get_game(game_key=str(pipeline_params.game_id))
        parser = GameParser(
            response=resp,
            season=pipeline_params.current_season,
            game_key=str(pipeline_params.game_id),
            data_key_list=end_point_params.data_key_list,
        )
        return resp, parser

    elif end_point_params.end_point == "get_league_preseason":
        resp, _ = yahoo_api.get_league_preseason(league_key=pipeline_params.league_key)
        parser = LeagueParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            league_key=pipeline_params.league_key,
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_league_draft_result":
        resp, _ = yahoo_api.get_league_draft_result(league_key=pipeline_params.league_key)
        parser = LeagueParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            league_key=pipeline_params.league_key,
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_league_matchup":
        resp, _ = yahoo_api.get_league_matchup(league_key=pipeline_params.league_key, week=pipeline_params.current_week)
        parser = LeagueParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            league_key=pipeline_params.league_key,
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_league_transaction":
        resp, _ = yahoo_api.get_league_transaction(league_key=pipeline_params.league_key)
        parser = LeagueParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            league_key=pipeline_params.league_key,
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_league_offseason":
        resp, _ = yahoo_api.get_league_offseason(league_key=pipeline_params.league_key)
        parser = LeagueParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            league_key=pipeline_params.league_key,
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_roster":
        resp, _ = yahoo_api.get_roster(team_key_list=pipeline_params.team_key_list, week=pipeline_params.current_week)
        parser = TeamParser(
            response=resp,  # type: ignore
            season=pipeline_params.current_season,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_player":
        resp, _ = yahoo_api.get_player(
            league_key=pipeline_params.league_key,
            start_count=end_point_params.page_start,  # type: ignore
            retrieval_limit=end_point_params.retrieval_limit,  # type: ignore
        )
        parser = PlayerParser(
            response=resp,  # type: ignore
            league_key=pipeline_params.league_key,
            season=pipeline_params.current_season,
            start=end_point_params.start,  # type: ignore
            end=end_point_params.end,  # type: ignore
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser  # type: ignore

    elif end_point_params.end_point == "get_player_draft_analysis":
        resp, _ = yahoo_api.get_player_draft_analysis(
            league_key=pipeline_params.league_key, player_key_list=end_point_params.player_key_list  # type: ignore
        )
        parser = PlayerParser(
            response=resp,  # type: ignore
            league_key=pipeline_params.league_key,
            season=pipeline_params.current_season,
            start=end_point_params.start,  # type: ignore
            end=end_point_params.end,  # type: ignore
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_player_stat":
        resp, _ = yahoo_api.get_player_stat(
            league_key=pipeline_params.league_key,
            player_key_list=end_point_params.player_key_list,  # type: ignore
            week=pipeline_params.current_week,
        )
        parser = PlayerParser(
            response=resp,  # type: ignore
            league_key=pipeline_params.league_key,
            season=pipeline_params.current_season,
            start=end_point_params.start,  # type: ignore
            end=end_point_params.end,  # type: ignore
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser

    elif end_point_params.end_point == "get_player_pct_owned":
        resp, _ = yahoo_api.get_player_pct_owned(
            league_key=pipeline_params.league_key,
            player_key_list=end_point_params.player_key_list,  # type: ignore
            week=pipeline_params.current_week,
        )
        parser = PlayerParser(
            response=resp,  # type: ignore
            league_key=pipeline_params.league_key,
            season=pipeline_params.current_season,
            start=end_point_params.start,  # type: ignore
            end=end_point_params.end,  # type: ignore
            end_point=end_point_params.end_point,
            week=str(pipeline_params.current_week),
        )
        return resp, parser


@task
def parse_response(data_parser: YahooParseBase, end_point: str) -> dict[str, DataFrame]:
    logger = get_run_logger()
    parsing_methods = get_parsing_methods(end_point, data_parser)
    logger.info(f"Parsing method for {end_point} retrieved.")

    df_dict = {}
    for parse_name, parse_method in parsing_methods.items():
        mapped_table = END_POINT_TABLE_MAP[f"{end_point}_{parse_name}"]
        df_dict.update({mapped_table: parse_method()})

    dict_len = len(df_dict)
    logger.info(f"Number of tables returned: {dict_len}.")
    return df_dict


@task
def json_to_db(raw_data: dict, db_params: DatabaseParameters, columns: list[str] | None = None) -> None:
    """
    Copy data into postgres
    """
    logger = get_run_logger()  # type: ignore
    # schema_name = database_parameters.schema_name
    schema_name = "yahoo_json"
    set_schema_statement = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))
    logger.info(f"Json load to table {schema_name}.{db_params.table_name}.")

    copy_statement = "COPY {0} ({1}) FROM STDIN"
    column_names = [sql.Identifier(col) for col in columns] if columns else [sql.Identifier("yahoo_json")]
    copy_query = sql.SQL(copy_statement).format(sql.Identifier(db_params.table_name), *column_names)  # type: ignore
    logger.info(f"SQL Copy Statement:\n\t{copy_query}")

    file_buffer = io.StringIO()  # type: ignore
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
    logger = get_run_logger()  # type: ignore
    schema_name = "yahoo_data"
    set_schema_statement = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))
    logger.info(f"Json load to table {schema_name}.{db_params.table_name}.")

    copy_statement = "COPY {table_name} ({column_names}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')"
    column_names = sql.SQL(", ").join([sql.Identifier(col) for col in resp_table_df.columns])
    copy_query = sql.SQL(copy_statement).format(
        table_name=sql.Identifier(db_params.table_name), column_names=column_names  # type: ignore
    )
    logger.info(f"SQL Copy Statement:\n\t{copy_query}")

    file_buffer = io.BytesIO()
    resp_table_df.write_csv(file_buffer, has_header=True, separator=",", line_terminator="\n", quote_style="always")
    file_buffer.seek(0)

    conn = psycopg.connect(db_params.db_conn_uri.get_secret_value())
    logger.info("Connection to postgres database successful.")

    try:
        curs = conn.cursor()
        curs.execute(set_schema_statement)

        with curs.copy(copy_query) as copy:
            copy.write(file_buffer.read())

        status_msg = curs.statusmessage
        logger.info(f"Parsed dataframe copied successfully.\n\t{status_msg}")

    except (Exception, psycopg.DatabaseError) as error:  # type: ignore
        logger.exception(f"Error with database:\n\n{error}\n\n")
        conn.rollback()
        raise error

    finally:
        conn.commit()
        conn.close()
        logger.info("Postgres connection closed.")


@task
def get_yahoo_api_config(how_many_conig: int) -> Config | list[Config]:
    logger = get_run_logger()  # type: ignore
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
    logger.info("Retrieved yahoo api configurations.")
    return config_return
