import os
from datetime import datetime

from dotenv import load_dotenv
from polars import DataFrame
from prefect import flow
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import construct_schedule
from prefect.task_runners import SequentialTaskRunner
from pydantic import SecretStr
from pytz import timezone

from prefect_orchestration.modules.prefect_blocks import get_file_from_bucket, notify_discord
from prefect_orchestration.modules.utils import (
    PARSE_CLASS,
    PipelineConfiguration,
    PipelineParameters,
    chunk_list_twenty_five,
    define_pipeline_schedules,
    determine_end_points,
    df_to_db,
    extractor,
    get_parameters,
    get_pipeline_config,
    get_player_key_list,
    get_week,
    json_to_db,
    parse_response,
    split_pipelines,
)

load_dotenv()

ENV_STATUS = os.getenv("ENVIRONMENT", "local")


@flow(on_failure=[notify_discord])
def get_list_of_pipeline_config(
    pipeline_params: PipelineParameters,
    start_count: int | None = None,
    retrieval_limit: int | None = None,
) -> list[PipelineConfiguration]:
    try:
        start_count = start_count if start_count else 0
        retrieval_limit = retrieval_limit if retrieval_limit else 25
        set_end_points = determine_end_points(pipeline_params)

        pipe_config_list = []
        for end_point in set_end_points:
            if end_point == "get_player":
                for page_start in range(start_count, 2000, retrieval_limit):
                    pipe_config_list.append(
                        get_pipeline_config(
                            pipeline_params=pipeline_params,
                            end_point=end_point,
                            page_start=page_start,
                            retrieval_limit=retrieval_limit,
                            player_key_list=None,
                        )
                    )

            elif end_point in ["get_player_draft_analysis", "get_player_stat", "get_player_pct_owned"]:
                player_key_list = get_player_key_list(pipeline_params)
                player_chunks = chunk_list_twenty_five(player_key_list)
                for chunked_player_list in player_chunks:
                    pipe_config_list.append(
                        get_pipeline_config(
                            pipeline_params=pipeline_params,
                            end_point=end_point,
                            page_start=None,
                            retrieval_limit=None,
                            player_key_list=chunked_player_list,
                        )
                    )

            else:
                pipe_config_list.append(
                    get_pipeline_config(
                        pipeline_params=pipeline_params,
                        end_point=end_point,
                        page_start=None,
                        retrieval_limit=None,
                        player_key_list=None,
                    )
                )
        return pipe_config_list

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def extract_data(pipeline_config: PipelineConfiguration) -> tuple[dict, str, PARSE_CLASS, str]:
    try:
        resp, query_ts, data_parser = extractor(pipeline_config)
        return resp, query_ts, data_parser, pipeline_config.end_point

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def parse_data(data_parser: PARSE_CLASS, end_point: str) -> dict[str, DataFrame]:
    try:
        data = parse_response(data_parser, end_point)
        return data

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def load_raw_data(data_dict: dict, params_dict: PipelineParameters, columns: list[str]) -> bool:
    try:
        json_to_db(data_dict, params_dict, columns)
        return True
    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def load_parsed_data(data_df: DataFrame, params_dict: PipelineParameters) -> bool:
    try:
        df_to_db(data_df, params_dict)
        return True
    except Exception as e:
        raise e


@flow(task_runner=SequentialTaskRunner(), on_failure=[notify_discord])
def load_pipeline_list(
    current_date: datetime,
    season: int,
    game_id: int,
    league_id: int,
    schema_name: str,
    num_of_teams: int,
) -> tuple[list[PipelineConfiguration], list[PipelineConfiguration] | None, list[PipelineConfiguration] | None]:
    try:
        flow_params = get_parameters(
            current_date=current_date,
            num_of_teams=num_of_teams,
            season=season,
            game_id=game_id,
            league_id=league_id,
            schema_name=schema_name,
            table_name="",
        )
        list_of_pipelines = get_list_of_pipeline_config(
            pipeline_params=flow_params, start_count=None, retrieval_limit=None
        )
        chunked_pipelines = split_pipelines(pipe_config_list=list_of_pipelines)

        return chunked_pipelines

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def extract_transform_load(pipeline_config: PipelineConfiguration) -> bool:
    try:
        resp, _, data_parser, end_point = extract_data(pipeline_config)

        pipeline_config.pipeline_params.db_params.schema_name = "yahoo_json"
        load_raw = load_raw_data(  # noqa: F841
            data_dict=resp, params_dict=pipeline_config.pipeline_params, columns=["yahoo_json"]
        )

        dict_of_data = parse_data(data_parser=data_parser, end_point=end_point)
        pipeline_config.pipeline_params.db_params.schema_name = "yahoo_data"
        load_parse = []
        for table_name, table_df in dict_of_data.items():
            pipeline_config.pipeline_params.db_params.table_name = table_name
            load_parse.append(
                load_parsed_data.submit(  # type: ignore
                    data_df=table_df,
                    params_dict=pipeline_config.pipeline_params,
                )
            )

        parsed_load = [i for p in load_parse for i in p.result()]  # noqa: F841
        return True

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def sunday_yahoo_flow(
    current_date: datetime,
    season: int = 2023,
    game_id: int = 423,
    league_id: int = 127732,
    schema_name: str = "yahoo",
    num_of_teams: int = 10,
) -> bool:
    try:
        pipeline_chunks = load_pipeline_list(
            current_date,
            season,
            game_id,
            league_id,
            schema_name,
            num_of_teams,
        )
        pipelines = []
        db_conn_uri = (
            os.getenv("SUPABASE_CONN_URI_YAHOO") if ENV_STATUS == "local" else Secret.load("supabase-conn-uri").get()  # type: ignore
        )

        if pipeline_chunks[1] and pipeline_chunks[2]:
            for chunk_one, chunk_two, chunk_three in zip(
                pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2], strict=True
            ):
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

                key_two = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_two = (
                    os.getenv("YAHOO_CONSUMER_SECRET_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-two").get()  # type: ignore
                )
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_two)  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_two)  # type: ignore
                chunk_two.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_two.yaml"

                get_file_from_bucket("oauth_token_two.yaml")
                pipe_two = extract_transform_load.submit(chunk_two)  # type: ignore
                pipelines.append(pipe_two)

                key_three = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_three = (
                    os.getenv("YAHOO_CONSUMER_SECRET_THREE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-three").get()  # type: ignore
                )
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_three)  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_three)  # type: ignore
                chunk_three.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_three.yaml"

                get_file_from_bucket("oauth_token_three.yaml")
                pipe_three = extract_transform_load.submit(chunk_three)  # type: ignore
                pipelines.append(pipe_three)

        else:
            for chunk_one in pipeline_chunks[0]:
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

        all_pipelines = [i for p in pipelines for i in p.result()]  # noqa: F841

        return True

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def weekly_yahoo_flow(
    current_date: datetime,
    season: int = 2023,
    game_id: int = 423,
    league_id: int = 127732,
    schema_name: str = "yahoo",
    num_of_teams: int = 10,
) -> bool:
    try:
        pipeline_chunks = load_pipeline_list(
            current_date,
            season,
            game_id,
            league_id,
            schema_name,
            num_of_teams,
        )
        pipelines = []
        db_conn_uri = (
            os.getenv("SUPABASE_CONN_URI_YAHOO") if ENV_STATUS == "local" else Secret.load("supabase-conn-uri").get()  # type: ignore
        )

        if pipeline_chunks[1] and pipeline_chunks[2]:
            for chunk_one, chunk_two, chunk_three in zip(
                pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2], strict=True
            ):
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

                key_two = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_two = (
                    os.getenv("YAHOO_CONSUMER_SECRET_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-two").get()  # type: ignore
                )
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_two)  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_two)  # type: ignore
                chunk_two.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_two.yaml"

                get_file_from_bucket("oauth_token_two.yaml")
                pipe_two = extract_transform_load.submit(chunk_two)  # type: ignore
                pipelines.append(pipe_two)

                key_three = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_three = (
                    os.getenv("YAHOO_CONSUMER_SECRET_THREE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-three").get()  # type: ignore
                )
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_three)  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_three)  # type: ignore
                chunk_three.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_three.yaml"

                get_file_from_bucket("oauth_token_three.yaml")
                pipe_three = extract_transform_load.submit(chunk_three)  # type: ignore
                pipelines.append(pipe_three)

        else:
            for chunk_one in pipeline_chunks[0]:
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

        all_pipelines = [i for p in pipelines for i in p.result()]  # noqa: F841

        return True

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord])
def off_pre_yahoo_flow(
    current_date: datetime,
    season: int = 2023,
    game_id: int = 423,
    league_id: int = 127732,
    schema_name: str = "yahoo",
    num_of_teams: int = 10,
) -> bool:
    try:
        pipeline_chunks = load_pipeline_list(
            current_date,
            season,
            game_id,
            league_id,
            schema_name,
            num_of_teams,
        )
        pipelines = []
        db_conn_uri = (
            os.getenv("SUPABASE_CONN_URI_YAHOO") if ENV_STATUS == "local" else Secret.load("supabase-conn-uri").get()  # type: ignore
        )

        if pipeline_chunks[1] and pipeline_chunks[2]:
            for chunk_one, chunk_two, chunk_three in zip(
                pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2], strict=True
            ):
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

                key_two = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_two = (
                    os.getenv("YAHOO_CONSUMER_SECRET_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-two").get()  # type: ignore
                )
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_two)  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_two)  # type: ignore
                chunk_two.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_two.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_two.yaml"

                get_file_from_bucket("oauth_token_two.yaml")
                pipe_two = extract_transform_load.submit(chunk_two)  # type: ignore
                pipelines.append(pipe_two)

                key_three = (
                    os.getenv("YAHOO_CONSUMER_KEY_TWO")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-two").get()  # type: ignore
                )
                secret_three = (
                    os.getenv("YAHOO_CONSUMER_SECRET_THREE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-three").get()  # type: ignore
                )
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_three)  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_three)  # type: ignore
                chunk_three.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_three.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_three.yaml"

                get_file_from_bucket("oauth_token_three.yaml")
                pipe_three = extract_transform_load.submit(chunk_three)  # type: ignore
                pipelines.append(pipe_three)

        else:
            for chunk_one in pipeline_chunks[0]:
                key_one = (
                    os.getenv("YAHOO_CONSUMER_KEY_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-key-one").get()  # type: ignore
                )
                secret_one = (
                    os.getenv("YAHOO_CONSUMER_SECRET_ONE")
                    if ENV_STATUS == "local"
                    else Secret.load("yahoo-consumer-secret-one").get()  # type: ignore
                )
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_key = SecretStr(key_one)  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.yahoo_consumer_secret = SecretStr(secret_one)  # type: ignore
                chunk_one.pipeline_params.db_params.db_conn_uri = db_conn_uri  # type: ignore
                chunk_one.pipeline_params.yahoo_export_config.token_file_path = "oauth_token_one.yaml"

                get_file_from_bucket("oauth_token_one.yaml")
                pipe_one = extract_transform_load.submit(chunk_one)  # type: ignore
                pipelines.append(pipe_one)

        all_pipelines = [i for p in pipelines for i in p.result()]  # noqa: F841

        return True

    except Exception as e:
        raise e


if __name__ == "__main__":
    current_date = datetime.now(tz=timezone("UTC"))
    anchor_timezone = "America/Denver"
    nfl_season = get_week(current_date, True)

    sunday_rrule_str, weekly_rrule_str, off_pre_rrule_str = define_pipeline_schedules()
    sunday_schedule = construct_schedule(rrule=sunday_rrule_str, timezone=anchor_timezone)
    weekly_schedule = construct_schedule(rrule=weekly_rrule_str, timezone=anchor_timezone)
    off_pre_schedule = construct_schedule(rrule=off_pre_rrule_str, timezone=anchor_timezone)
    sunday_flow = sunday_yahoo_flow.to_deployment(  # type: ignore
        name="sunday-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the regular-season.",
        schedule=sunday_schedule,
        parameters={"current_date": current_date},
    )
    weekly_flow = weekly_yahoo_flow.to_deployment(  # type: ignore
        name="weekly-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the post-season.",
        schedule=weekly_schedule,
        parameters={"current_date": current_date},
    )
    off_pre_flow = off_pre_yahoo_flow.to_deployment(  # type: ignore
        name="off-pre-season-flow",
        description="Export league data from Yahoo Fantasy Sports API to Supabase during the off-season.",
        schedule=off_pre_schedule,
        parameters={"current_date": current_date},
    )
    serve(sunday_flow, weekly_flow, off_pre_flow)
