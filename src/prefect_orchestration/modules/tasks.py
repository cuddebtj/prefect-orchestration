import io
import json
import math
import os
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal

import polars as pl
import psycopg
from polars import DataFrame
from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from psycopg import Connection, sql
from pydantic import SecretStr
from pytz import timezone
from yahoo_export import Config, YahooAPI
from yahoo_parser import (
    GameParser,
    LeagueParser,
    PlayerParser,
    TeamParser,
    YahooParseBase,
)

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
    get_data_from_db,
    get_labor_day,
    get_parsing_methods,
    get_week,
)


@task
def get_run_datetime(run_datetime: str) -> datetime:
    logger = get_run_logger()
    logger.info(f"Run datetime provided: {run_datetime}")
    if run_datetime != "":
        run_timestamp = datetime.fromisoformat(run_datetime).astimezone(timezone("UTC"))
    else:
        run_timestamp = datetime.now(tz=timezone("UTC"))

    return run_timestamp


@task
def determine_end_points(pipeline_params: PipelineParameters) -> set[str]:
    logger = get_run_logger()
    labor_day = get_labor_day(pipeline_params.current_timestamp.astimezone(timezone("America/Denver")).date())
    nfl_season = get_week(pipeline_params.current_timestamp, get_all_weeks=True)
    current_week = pipeline_params.current_week
    nfl_start_date = nfl_season[0].week_start
    nfl_end_week = nfl_season[-1].week
    current_date = pipeline_params.current_timestamp.astimezone(timezone("America/Denver")).date()  # type: ignore
    current_day_of_week = current_date.weekday()  # type: ignore

    end_points = []

    # preseason or offseason
    if current_week == OFFSEASON_WEEK:
        # preseason
        if current_date <= nfl_start_date and current_date > labor_day:  # type: ignore
            # draft results, team info, league info
            end_points += PRESEASON_END_POINTS
            # week 1 matchups
            end_points += BEGINNING_OF_WEEK_END_POINTS

        # offseason
        else:
            end_points += OFFSEASON_END_POINTS

    # regular season -> live or weekly
    if current_week > OFFSEASON_WEEK and current_week < nfl_end_week:
        # get prior week score adjustments and next week matchups
        if current_day_of_week == TUESDAY:
            # matchups and player stats
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
def get_player_key_list(db_conn: Connection, league_key: str) -> list[str]:
    logger = get_run_logger()
    sql_str = """
        select distinct player_key
        from yahoo_data.players
        where league_key = {league_key}
          and coalesce(player_key, '') != ''
        """
    logger.info("Getting player key list from database.")
    sql_query = sql.SQL(sql_str).format(league_key=sql.Literal(league_key))
    player_key_list = [player_key[0] for player_key in get_data_from_db(db_conn, sql_query)]
    logger.info(f"Returning player key's {len(player_key_list)}.")
    return player_key_list


@task
def split_pipelines(
    end_point_list: list[EndPointParameters],
) -> tuple[
    list[EndPointParameters],
    list[EndPointParameters] | None,
    list[EndPointParameters] | None,
]:
    logger = get_run_logger()
    pipeline_length = len(end_point_list)
    logger.info(f"Number of pipelines to be run {pipeline_length!s}")

    if pipeline_length >= 3:  # noqa: PLR2004
        chunk_size = math.ceil(pipeline_length / 3)
        logger.info(f"Pipeline chunk sizes {chunk_size!s}")
        chunk_one = end_point_list[:chunk_size]
        logger.info(f"Pipelines chunk_one size {len(chunk_one)!s}")  # type: ignore
        chunk_two = end_point_list[chunk_size : chunk_size * 2]
        logger.info(f"Pipelines chunk_two size {len(chunk_two)!s}")  # type: ignore
        chunk_three = end_point_list[chunk_size * 2 :]
        logger.info(f"Pipelines chunk_three size {len(chunk_three)!s}")  # type: ignore

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
def get_sleeper_player_projection_data(season: int | str, week: int | str) -> Sequence[dict[Any, Any]]:
    import requests

    logger = get_run_logger()

    sleeper_base_url = "https://api.sleeper.app/"
    sleeper_projections_url = (
        sleeper_base_url
        + f"/projections/nfl/{season}/{week}"
        + "?season_type=regular&position[]=DB&position[]=DEF"
        + "&position[]=DL&position[]=FLEX&position[]=IDP_FLEX"
        + "&position[]=K&position[]=LB&position[]=QB"
        + "&position[]=RB&position[]=REC_FLEX&position[]=SUPER_FLEX"
        + "&position[]=TE&position[]=WR&position[]=WRRB_FLEX&order_by=ppr"
    )

    logger.info(f"Getting sleeper player projections for {season} and {week}.")
    with requests.Session() as session:
        try:
            response = session.get(sleeper_projections_url)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as err:
            logger.exception(f"Error with sleeper request:\n\n{err}\n\n", exc_info=True)
            raise err

        except requests.exceptions.RequestException as err:
            logger.exception(f"Error with sleeper request:\n\n{err}\n\n", exc_info=True)
            raise err


@task
def parse_sleeper_player_projections_data(
    response_data: Sequence[dict[str, Any]]
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    info_df = pl.DataFrame()
    player_df = pl.DataFrame()
    player_metadata_df = pl.DataFrame()
    stats_df = pl.DataFrame()

    response_data = sorted(response_data, key=lambda player_row: player_row["player_id"], reverse=True)
    for player_row in response_data:
        player_row["stats"].update(
            {
                "player_id": player_row["player_id"],
                "week": player_row["week"],
            }
        )
        player_row["player"].update(
            {
                "player_id": player_row["player_id"],
                "week": player_row["week"],
            }
        )
        if player_row["player"]["metadata"] is None:
            player_row["player"]["metadata"] = {
                "player_id": player_row["player_id"],
                "week": player_row["week"],
            }
        else:
            player_row["player"]["metadata"].update(
                {
                    "player_id": player_row["player_id"],
                    "week": player_row["week"],
                }
            )

        info_dict = {
            "opponent": player_row["opponent"],
            "company": player_row["company"],
            "team": player_row["team"],
            "player_id": player_row["player_id"],
            "game_id": player_row["game_id"],
            "sport": player_row["sport"],
            "season_type": player_row["season_type"],
            "season": player_row["season"],
            "week": player_row["week"],
            "category": player_row["category"],
            "date": player_row["date"],
        }
        player_dict = player_row["player"].copy()
        player_metadata_dict = player_row["player"]["metadata"].copy()

        final_player_metadata_dict = {}
        for key in player_metadata_dict.keys():
            if "injury_override_" in key:
                new_key = key[:15]
                new_key_value = key[16:]
                final_player_metadata_dict[new_key] = new_key_value
            else:
                final_player_metadata_dict[key] = player_metadata_dict[key]

        stats_dict = player_row["stats"].copy()

        del player_dict["metadata"]

        info_row_df = pl.from_dict(info_dict)
        info_df = pl.concat([info_row_df, info_df], how="diagonal_relaxed")

        player_row_df = pl.from_dict(player_dict)
        player_df = pl.concat([player_row_df, player_df], how="diagonal_relaxed")

        player_metadata_row_df = pl.from_dict(final_player_metadata_dict)
        player_metadata_df = pl.concat([player_metadata_row_df, player_metadata_df], how="diagonal_relaxed")

        stats_row_df = pl.from_dict(stats_dict)
        stats_df = pl.concat([stats_row_df, stats_df], how="diagonal_relaxed")

    return info_df, player_df, player_metadata_df, stats_df


@task
def get_sleeper_player_info_data() -> dict[Any, Any]:
    import requests

    logger = get_run_logger()

    player_info_url = "https://api.sleeper.app/v1/players/nfl"

    logger.info("Getting sleeper player info.")
    with requests.Session() as session:
        try:
            response = session.get(player_info_url)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as err:
            logger.exception(f"Error with sleeper request:\n\n{err}\n\n", exc_info=True)
            raise err

        except requests.exceptions.RequestException as err:
            logger.exception(f"Error with sleeper request:\n\n{err}\n\n", exc_info=True)
            raise err


@task
def parse_sleeper_player_info_data(response_data: dict[str, Any], week: str | int) -> tuple[DataFrame, DataFrame]:
    info_dict = []
    metadata_dict = []
    for key, value in response_data.items():
        value.update({"sleeper_id": key})
        value.update({"week": str(week)})
        value["fantasy_positions"] = (
            ", ".join(value["fantasy_positions"])
            if isinstance(value["fantasy_positions"], list)
            else value["fantasy_positions"]
        )

        if value.get("metadata", None) is None:
            value["metadata"] = {"sleeper_id": key, "week": str(week)}
        else:
            value["metadata"].update({"sleeper_id": key, "week": str(week)})

        metadata = value["metadata"].copy()

        del value["metadata"]

        final_metadata_dict = {}
        for meta_key in metadata.keys():
            if "injury_override_" in meta_key:
                new_key = meta_key[:15]
                new_key_value = meta_key[16:]
                final_metadata_dict[new_key] = new_key_value
            else:
                final_metadata_dict[meta_key] = metadata[meta_key]

        metadata_dict.append(final_metadata_dict)
        info_dict.append(value)

    info_df = pl.from_dicts(info_dict, infer_schema_length=10000)
    meta_df = pl.from_dicts(metadata_dict, infer_schema_length=10000)

    return info_df, meta_df


@task
def extractor(
    pipeline_params: PipelineParameters,
    end_point_params: EndPointParameters,
    yahoo_api: YahooAPI,
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
        resp, _ = yahoo_api.get_roster(
            team_key_list=pipeline_params.team_key_list,
            week=pipeline_params.current_week,
        )
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
            league_key=pipeline_params.league_key,
            player_key_list=end_point_params.player_key_list,  # type: ignore
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
def data_to_db(
    resp_data: dict | DataFrame,
    db_params: DatabaseParameters,
    json_or_df: Literal["json", "df"],
    schema_name: str | None = None,
) -> None:
    """
    Copy data into postgres
    """
    logger = get_run_logger()  # type: ignore

    if json_or_df == "json":
        schema_name = "yahoo_json"
        columns = ["json_data"]
        logger.info(f"Json load to table {schema_name}.{db_params.table_name}.")
        copy_statement = """COPY {table_name} ({column_names})
        FROM STDIN"""

        file_buffer = io.StringIO()  # type: ignore
        json.dump(resp_data, file_buffer)  # type: ignore
        file_buffer.seek(0)

    elif json_or_df == "df":
        schema_name = "yahoo_data" if not schema_name else schema_name
        columns = resp_data.columns  # type: ignore
        logger.info(f"Dataframe CSV load to table {schema_name}.{db_params.table_name}.")
        copy_statement = """COPY {table_name} ({column_names})
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')"""

        file_buffer = io.BytesIO()
        resp_data.write_csv(file_buffer, has_header=True, separator=",", line_terminator="\n", quote_style="always")  # type: ignore
        file_buffer.seek(0)

    set_delete_statement = sql.SQL("CALL yahoo_data.delete_duplicate_data({schema_name}, {table_name});").format(
        schema_name=sql.Literal(schema_name),
        table_name=sql.Literal(db_params.table_name),
    )
    logger.info(f"SQL Delete Statement:\n\t{set_delete_statement}")

    set_schema_statement = sql.SQL("set search_path to {};").format(sql.Identifier(schema_name))

    column_names = sql.SQL(", ").join([sql.Identifier(col) for col in columns])
    copy_query = sql.SQL(copy_statement).format(
        table_name=sql.Identifier(db_params.table_name),
        column_names=column_names,  # type: ignore
    )

    logger.info(f"SQL Copy Statement:\n\t{copy_query}")

    try:
        curs = db_params.db_conn.cursor()
        curs.execute(set_schema_statement)

        with curs.copy(copy_query) as copy:
            copy.write(file_buffer.read())

        if json_or_df == "df":
            curs.execute(set_delete_statement)

        status_msg = curs.statusmessage
        logger.info(f"Response copied successfully.\n\t{status_msg}")

    except (Exception, psycopg.DatabaseError) as error:  # type: ignore
        logger.exception(f"Error with database:\n\n{error}\n\n", exc_info=True, stack_info=True)
        db_params.db_conn.rollback()
        logger.info("Postgres transaction rolled back.")
        raise error

    finally:
        db_params.db_conn.commit()
        logger.info("Postgres transaction commited.")


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
                os.getenv(
                    f"YAHOO_CONSUMER_KEY_{config_num_str.upper()}",
                    f"key_{config_num_str}",
                )
                if env_status == "local"
                else Secret.load(f"yahoo-consumer-key-{config_num_str}").get()  # type: ignore
            )
            consumer_secret = SecretStr(
                os.getenv(
                    f"YAHOO_CONSUMER_SECRET_{config_num_str.upper()}",
                    f"secret_{config_num_str}",
                )
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
