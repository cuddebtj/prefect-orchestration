import os
from dataclasses import asdict
from datetime import datetime
from itertools import zip_longest

import psycopg
from prefect import flow, get_run_logger, serve
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import construct_schedule
from prefect.task_runners import SequentialTaskRunner
from psycopg import Connection
from pydantic import SecretStr
from pytz import timezone
from yahoo_export import YahooAPI

from prefect_orchestration.modules.blocks import (
    get_file_from_bucket,
    notify_discord_cancellation,
    notify_discord_failure,
    upload_file_to_bucket,
)
from prefect_orchestration.modules.tasks import (
    data_to_db,
    determine_end_points,
    extractor,
    get_endpoint_config,
    get_player_key_list,
    get_run_datetime,
    get_sleeper_player_info_data,
    get_sleeper_player_projection_data,
    get_yahoo_api_config,
    parse_response,
    parse_sleeper_player_info_data,
    parse_sleeper_player_projections_data,
    split_pipelines,
)
from prefect_orchestration.modules.utils import (
    DatabaseParameters,
    EndPointParameters,
    PipelineParameters,
    chunk_to_twentyfive_items,
    define_pipeline_schedules,
    get_labor_day,
    get_week,
)

ENV_STATUS = None  # os.getenv("ENVIRONMENT", "local")


@flow(
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
)
def get_configuration_and_split_pipelines(
    db_conn: Connection,
    current_timestamp: datetime,
    game_id: int | str,
    league_id: int | str,
    num_of_teams: int,
    start_count: int = 0,
    retrieval_limit: int = 25,
) -> tuple[
    PipelineParameters,
    DatabaseParameters,
    tuple[list[EndPointParameters], list[EndPointParameters] | None, list[EndPointParameters] | None],
]:
    logger = get_run_logger()  # type: ignore
    try:
        pipeline_params = PipelineParameters(
            current_timestamp=current_timestamp,
            num_of_teams=num_of_teams,
            game_id=game_id,
            league_key=f"{game_id!s}.l.{league_id!s}",
        )
        logger.info(f"Pipeline Parameters set.\n{asdict(pipeline_params)}")
        set_end_points = determine_end_points(pipeline_params)

        logger.info("Determine list of endpoints:\n\t- {}".format("\n\t- ".join(set_end_points)))
        db_params = DatabaseParameters(db_conn=db_conn, schema_name=None, table_name=None)

        logger.info("Database parameters set.")
        end_point_list = []
        for end_point in set_end_points:
            if end_point == "get_player":
                logger.info("Get list of players.")
                for page_start in range(start_count, 2000, retrieval_limit):
                    end_point_config = get_endpoint_config.submit(
                        end_point=end_point,
                        page_start=page_start,
                        retrieval_limit=retrieval_limit,
                        player_key_list=None,
                        wait_for=[set_end_points],
                    )  # type: ignore
                    end_point_list.append(end_point_config)

            elif end_point in [
                "get_player_draft_analysis",
                "get_player_stat",
                "get_player_pct_owned",
            ]:
                logger.info("Get player info after having player list live data end points.")
                player_key_list = get_player_key_list(db_params.db_conn, pipeline_params.league_key)

                logger.info(f"Row counts returend: {len(player_key_list)}.")

                player_chunks = chunk_to_twentyfive_items(player_key_list)

                for chunked_player_list in player_chunks:
                    end_point_config = get_endpoint_config.submit(
                        end_point=end_point,
                        page_start=None,
                        retrieval_limit=None,
                        player_key_list=chunked_player_list,
                        wait_for=[player_chunks],
                    )  # type: ignore
                    end_point_list.append(end_point_config)

            else:
                logger.info("Non player info end points.")
                end_point_config = get_endpoint_config.submit(
                    end_point=end_point,
                    page_start=None,
                    retrieval_limit=None,
                    player_key_list=None,
                    wait_for=[set_end_points],
                )  # type: ignore
                end_point_list.append(end_point_config)

        end_point_list = [x.result() for x in end_point_list]
        chunked_pipelines = split_pipelines(end_point_list=end_point_list)
        logger.info("Pipelines split into chunks")

        return pipeline_params, db_params, chunked_pipelines

    except Exception as e:
        raise e


@flow(
    validate_parameters=False,
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
    task_runner=SequentialTaskRunner(),
)
def extract_transform_load(
    pipeline_params: PipelineParameters,
    db_params: DatabaseParameters,
    end_point_param: EndPointParameters,
    yahoo_api: YahooAPI,
) -> bool:
    logger = get_run_logger()  # type: ignore
    logger.info("Extracting data from Yahoo API.")
    resp, data_parser = extractor(pipeline_params, end_point_param, yahoo_api)  # type: ignore
    logger.info(f"Extracting from end_point {end_point_param.end_point} successfull.")

    db_params.schema_name = "yahoo_json"
    db_params.table_name = end_point_param.end_point.replace("get_", "")
    logger.info("Writing raw data to database.")
    data_to_db(resp_data=resp, db_params=db_params, json_or_df="json")

    db_params.schema_name = "yahoo_data"
    db_params.table_name = None
    logger.info("Parsing raw data to tables.")
    parsed_data = parse_response(data_parser, end_point_param.end_point)  # type: ignore

    logger.info("Writing tables to database.")
    for table_name, table_df in parsed_data.items():
        db_params.table_name = table_name
        data_to_db(resp_data=table_df, db_params=db_params, json_or_df="df")

    return True


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def yahoo_flow(
    run_datetime: str = "",
    game_id: int = 423,
    league_id: int = 127732,
    num_of_teams: int = 10,
) -> bool:
    logger = get_run_logger()  # type: ignore
    current_timestamp = get_run_datetime(run_datetime)
    try:
        connection_string = SecretStr(
            os.getenv("SUPABASE_CONN_PYTHON", "localhost")
            if ENV_STATUS == "local"
            else Secret.load("supabase-conn-python").get()  # type: ignore
        )
        db_conn = psycopg.connect(connection_string.get_secret_value())

    except psycopg.DatabaseError as connection_error:
        logger.exception(connection_error, exc_info=True, stack_info=True)
        raise connection_error

    except Exception as error:
        logger.exception(error, exc_info=True, stack_info=True)
        raise error

    else:
        logger.info("Database connection established.")

        pipeline_params, db_params, pipeline_chunks = get_configuration_and_split_pipelines(
            db_conn=db_conn,
            current_timestamp=current_timestamp,
            game_id=game_id,
            league_id=league_id,
            num_of_teams=num_of_teams,
        )
        logger.info("Successfully retirved pipeline configurations.")

        pipelines = []
        if pipeline_chunks[1] and pipeline_chunks[2]:
            logger.info("More than 25 end points to query.")
            yahoo_config_list = get_yahoo_api_config(3)

            get_file_from_bucket(yahoo_config_list[0].token_file_path)  # type: ignore
            get_file_from_bucket(yahoo_config_list[1].token_file_path)  # type: ignore
            get_file_from_bucket(yahoo_config_list[2].token_file_path)  # type: ignore
            logger.info("Retrived token files from google.")

            yahoo_api_one = YahooAPI(config=yahoo_config_list[0])  # type: ignore
            yahoo_api_two = YahooAPI(config=yahoo_config_list[1])  # type: ignore
            yahoo_api_three = YahooAPI(config=yahoo_config_list[2])  # type: ignore
            logger.info("YahooAPI objects created.")
            zipped_chunks = zip_longest(pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2])

            try:
                for chunk_one, chunk_two, chunk_three in zipped_chunks:
                    if chunk_one:
                        pipe_one = extract_transform_load(pipeline_params, db_params, chunk_one, yahoo_api_one)  # type: ignore
                        pipelines.append(pipe_one)

                    if chunk_two:
                        pipe_two = extract_transform_load(pipeline_params, db_params, chunk_two, yahoo_api_two)  # type: ignore
                        pipelines.append(pipe_two)

                    if chunk_three:
                        pipe_three = extract_transform_load(  # type: ignore
                            pipeline_params, db_params, chunk_three, yahoo_api_three
                        )
                        pipelines.append(pipe_three)

                logger.info("Successfull ETL on yahoo data.")

            except Exception as e:
                logger.error(e, exc_info=True, stack_info=True)

            finally:
                upload_file_to_bucket(yahoo_config_list[0].token_file_path)  # type: ignore
                upload_file_to_bucket(yahoo_config_list[1].token_file_path)  # type: ignore
                upload_file_to_bucket(yahoo_config_list[2].token_file_path)  # type: ignore
                logger.info("Updated token files to google.")

        else:
            logger.info("Less than 25 end points to query.")
            yahoo_config_list = get_yahoo_api_config(1)
            get_file_from_bucket(yahoo_config_list.token_file_path)  # type: ignore
            logger.info("Retrived token files from google.")
            yahoo_api_one = YahooAPI(config=yahoo_config_list)  # type: ignore
            logger.info("YahooAPI objects created.")

            for chunk_one in pipeline_chunks[0]:
                pipe_one = extract_transform_load(pipeline_params, db_params, chunk_one, yahoo_api_one)  # type: ignore
                pipelines.append(pipe_one)

            logger.info("Successfull ETL on yahoo data.")
            upload_file_to_bucket(yahoo_config_list.token_file_path)  # type: ignore
            logger.info("Updated token files to google.")

        return True

    finally:
        db_conn.close()  # type: ignore


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def sleeper_flow(
    run_datetime: str = "",
) -> bool:
    logger = get_run_logger()  # type: ignore
    current_timestamp = get_run_datetime(run_datetime)
    try:
        connection_string = SecretStr(
            os.getenv("SUPABASE_CONN_PYTHON", "localhost")
            if ENV_STATUS == "local"
            else Secret.load("supabase-conn-python").get()  # type: ignore
        )
        db_conn = psycopg.connect(connection_string.get_secret_value())

    except psycopg.DatabaseError as connection_error:
        logger.exception(connection_error, exc_info=True, stack_info=True)
        raise connection_error

    except Exception as error:
        logger.exception(error, exc_info=True, stack_info=True)
        raise error

    else:
        logger.info("Database connection established.")
        db_params = DatabaseParameters(db_conn=db_conn, schema_name="public", table_name=None)
        labor_day = get_labor_day(current_timestamp.date())
        season = labor_day.year
        nfl_week = get_week(current_timestamp)
        projection_data_resp = get_sleeper_player_projection_data(season, nfl_week.week)  # type: ignore
        player_info_resp = get_sleeper_player_info_data()

        projection_info, projection_player, projection_meta, projection_stats = parse_sleeper_player_projections_data(
            projection_data_resp
        )
        player_info, player_meta = parse_sleeper_player_info_data(player_info_resp, nfl_week.week)  # type: ignore

        db_params.table_name = "sleeper_player_projections"
        # for record in projection_data_resp:
        data_to_db(projection_data_resp, db_params, "json", db_params.schema_name)
        db_params.table_name = "sleeper_player_info"
        data_to_db(player_info_resp, db_params, "json", db_params.schema_name)

        db_params.table_name = "sleeper_player_projections_info"
        data_to_db(projection_info, db_params, "df", db_params.schema_name)
        db_params.table_name = "sleeper_player_projections_player"
        data_to_db(projection_player, db_params, "df", db_params.schema_name)
        db_params.table_name = "sleeper_player_projections_metadata"
        data_to_db(projection_meta, db_params, "df", db_params.schema_name)
        db_params.table_name = "sleeper_player_projections_stats"
        data_to_db(projection_stats, db_params, "df", db_params.schema_name)
        db_params.table_name = "sleeper_player_info"
        data_to_db(player_info, db_params, "df", db_params.schema_name)
        db_params.table_name = "sleeper_player_meatdata"
        data_to_db(player_meta, db_params, "df", db_params.schema_name)

        return True

    finally:
        db_conn.close()  # type: ignore


if __name__ == "__main__":
    anchor_timezone = "America/Denver"

    sunday_rrule_str, weekly_rrule_str, off_pre_rrule_str, once_wkly_rrule_str = define_pipeline_schedules(
        current_timestamp=datetime.now(tz=timezone("UTC"))
    )
    sunday_schedule = construct_schedule(rrule=sunday_rrule_str, timezone=anchor_timezone)
    weekly_schedule = construct_schedule(rrule=weekly_rrule_str, timezone=anchor_timezone)
    off_pre_schedule = construct_schedule(rrule=off_pre_rrule_str, timezone=anchor_timezone)
    once_wkly_schedule = construct_schedule(rrule=once_wkly_rrule_str, timezone=anchor_timezone)
    sunday_flow = yahoo_flow.to_deployment(  # type: ignore
        name="sunday-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the regular-season.",
        schedule=sunday_schedule,
        parameters={"run_datetime": ""},
        tags=["yahoo", "sunday", "live"],
    )
    weekly_flow = yahoo_flow.to_deployment(
        name="weekly-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the post-season.",
        schedule=weekly_schedule,
        parameters={"run_datetime": ""},
        tags=["yahoo", "weekly"],
    )
    off_pre_flow = yahoo_flow.to_deployment(
        name="off-pre-season-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the off-season.",
        schedule=off_pre_schedule,
        parameters={"run_datetime": ""},
        tags=["yahoo", "preseason", "offseason"],
    )
    sleeper_data_extraction = sleeper_flow.to_deployment(
        name="sleeper-data-extraction-sleeper-flow",
        description="Export player data from Sleeper API to Supabase weekly during the season.",
        schedule=once_wkly_schedule,
        parameters={"run_datetime": ""},
        tags=["sleeper", "weekly"],
    )
    serve(
        sunday_flow,  # type: ignore
        weekly_flow,
        off_pre_flow,
        sleeper_data_extraction,
    )
