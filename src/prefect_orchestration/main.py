import os
from datetime import datetime
from itertools import zip_longest

from prefect import flow, get_run_logger, serve
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import construct_schedule
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
    determine_end_points,
    df_to_db,
    extractor,
    get_endpoint_config,
    get_yahoo_api_config,
    json_to_db,
    parse_response,
    split_pipelines,
)
from prefect_orchestration.modules.utils import (
    DatabaseParameters,
    EndPointParameters,
    PipelineParameters,
    chunk_list_twenty_five,
    define_pipeline_schedules,
    get_player_key_list,
    get_week,
)

ENV_STATUS = None  # os.getenv("ENVIRONMENT", "local")


@flow(
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
)
def get_configuration_and_split_pipelines(
    db_conn_uri: SecretStr,
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
        logger.info("Pipeline Parameters set.")
        set_end_points = determine_end_points(pipeline_params)

        logger.info("Determine list of endpoints:\n\t{}".format("\n\t".join(set_end_points)))
        db_params = DatabaseParameters(db_conn_uri=db_conn_uri, schema_name=None, table_name=None)

        logger.info("Database parameters set.")
        end_point_list = []
        for end_point in set_end_points:
            if end_point == "get_player":
                logger.info("Get list of players.")
                for page_start in range(start_count, 2000, retrieval_limit):
                    end_point_list.append(
                        get_endpoint_config.submit(
                            end_point=end_point,
                            page_start=page_start,
                            retrieval_limit=retrieval_limit,
                            player_key_list=None,
                            wait_for=[set_end_points],
                        )  # type: ignore
                    )

            elif end_point in ["get_player_draft_analysis", "get_player_stat", "get_player_pct_owned"]:
                logger.info("Get player info after having player list live data end points.")
                player_key_list = get_player_key_list(db_params.db_conn_uri, pipeline_params.league_key)
                player_chunks = chunk_list_twenty_five(player_key_list)
                for chunked_player_list in player_chunks:
                    player_keys = [x[0] if isinstance(x, tuple) else x for x in chunked_player_list]
                    logger.info(f"Player Keys:\n{player_keys}\n")
                    end_point_list.append(
                        get_endpoint_config.submit(
                            end_point=end_point,
                            page_start=None,
                            retrieval_limit=None,
                            player_key_list=player_keys,
                            wait_for=[player_chunks, player_key_list],
                        )  # type: ignore
                    )

            else:
                logger.info("Non player info end points.")
                end_point_list.append(
                    get_endpoint_config.submit(
                        end_point=end_point,
                        page_start=None,
                        retrieval_limit=None,
                        player_key_list=None,
                        wait_for=[set_end_points],
                    )  # type: ignore
                )

        end_point_list = [x.result() for x in end_point_list]
        chunked_pipelines = split_pipelines(end_point_list=end_point_list)
        join_list = [str(len(x)) if x else "None" for x in chunked_pipelines]  # type: ignore
        logger_message = "\n\t".join(join_list)
        logger.info(f"Pipelines split into chunks:\n\t{logger_message}")

        return pipeline_params, db_params, chunked_pipelines

    except Exception as e:
        raise e


@flow(
    validate_parameters=False,
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
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
    json_to_db(raw_data=resp, db_params=db_params, columns=["yahoo_json"])

    db_params.schema_name = "yahoo_data"
    db_params.table_name = None
    logger.info("Parsing raw data to tables.")
    parsed_data = parse_response(data_parser, end_point_param.end_point)  # type: ignore

    logger.info("Writing tables to database.")
    for table_name, table_df in parsed_data.items():
        db_params.table_name = table_name
        df_to_db(resp_table_df=table_df, db_params=db_params)

    return True


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def yahoo_flow(
    current_timestamp: datetime,
    game_id: int = 423,
    league_id: int = 127732,
    num_of_teams: int = 10,
) -> bool:
    logger = get_run_logger()  # type: ignore
    try:
        db_conn_uri = SecretStr(
            os.getenv("SUPABASE_CONN_PYTHON", "localhost")
            if ENV_STATUS == "local"
            else Secret.load("supabase-conn-python").get()  # type: ignore
        )
        pipeline_params, db_params, pipeline_chunks = get_configuration_and_split_pipelines(
            db_conn_uri=db_conn_uri,
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
            logger.info(f"Pipeline One length: {len(pipeline_chunks[0])}")
            logger.info(f"Pipeline Two length: {len(pipeline_chunks[1])}")
            logger.info(f"Pipeline Three length: {len(pipeline_chunks[2])}")
            zipped_chunks = zip_longest(pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2])
            logger.info(f"Zipped chunks: \n\n{zipped_chunks}\n\n")

            try:
                for chunk_one, chunk_two, chunk_three in zipped_chunks:
                    if chunk_one:
                        pipe_one = extract_transform_load(pipeline_params, db_params, chunk_one, yahoo_api_one)
                        pipelines.append(pipe_one)

                    if chunk_two:
                        pipe_two = extract_transform_load(pipeline_params, db_params, chunk_two, yahoo_api_two)
                        pipelines.append(pipe_two)

                    if chunk_three:
                        pipe_three = extract_transform_load(pipeline_params, db_params, chunk_three, yahoo_api_three)
                        pipelines.append(pipe_three)

                logger.info("Successfull ETL on yahoo data.")
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

    except Exception as e:
        raise e


if __name__ == "__main__":
    current_timestamp = datetime.now(tz=timezone("UTC"))
    anchor_timezone = "America/Denver"
    nfl_season = get_week(current_timestamp, True)

    sunday_rrule_str, weekly_rrule_str, off_pre_rrule_str = define_pipeline_schedules(
        current_timestamp=current_timestamp
    )
    sunday_schedule = construct_schedule(rrule=sunday_rrule_str, timezone=anchor_timezone)
    weekly_schedule = construct_schedule(rrule=weekly_rrule_str, timezone=anchor_timezone)
    off_pre_schedule = construct_schedule(rrule=off_pre_rrule_str, timezone=anchor_timezone)
    sunday_flow = yahoo_flow.to_deployment(  # type: ignore
        name="sunday-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the regular-season.",
        schedule=sunday_schedule,
        parameters={"current_timestamp": current_timestamp},
        tags=["yahoo", "sunday", "live"],
    )
    weekly_flow = yahoo_flow.to_deployment(  # type: ignore
        name="weekly-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the post-season.",
        schedule=weekly_schedule,
        parameters={"current_timestamp": current_timestamp},
        tags=["yahoo", "weekly"],
    )
    off_pre_flow = yahoo_flow.to_deployment(  # type: ignore
        name="off-pre-season-yahoo-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the off-season.",
        schedule=off_pre_schedule,
        parameters={"current_timestamp": current_timestamp},
        tags=["yahoo", "preseason", "offseason"],
    )
    serve(
        sunday_flow,  # type: ignore
        weekly_flow,  # type: ignore
        off_pre_flow,  # type: ignore
    )
