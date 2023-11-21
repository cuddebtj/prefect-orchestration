import os
from datetime import datetime

from dotenv import load_dotenv
from polars import DataFrame
from prefect import flow, serve
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import construct_schedule
from prefect.task_runners import SequentialTaskRunner
from pydantic import SecretStr
from pytz import timezone
from yahoo_export import YahooAPI
from yahoo_parser import YahooParseBase

from prefect_orchestration.modules.prefect_blocks import (
    get_file_from_bucket,
    notify_discord_cancellation,
    notify_discord_failure,
    upload_file_to_bucket,
)
from prefect_orchestration.modules.utils import (
    DatabaseParameters,
    EndPointParameters,
    PipelineParameters,
    chunk_list_twenty_five,
    define_pipeline_schedules,
    determine_end_points,
    df_to_db,
    extractor,
    get_endpoint_config,
    get_player_key_list,
    get_week,
    get_yahoo_api_config,
    json_to_db,
    parse_response,
    split_pipelines,
)

load_dotenv()

ENV_STATUS = None  # os.getenv("ENVIRONMENT", "local")


@flow(
    task_runner=SequentialTaskRunner(),
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
)
def get_parameters(
    db_conn_uri: SecretStr,
    current_timestamp: datetime,
    game_id: int | str,
    league_id: int | str,
    num_of_teams: int,
    start_count: int = 0,
    retrieval_limit: int = 25,
) -> tuple[PipelineParameters, DatabaseParameters, list[EndPointParameters]]:
    try:
        pipeline_params = PipelineParameters(
            current_timestamp=current_timestamp,
            num_of_teams=num_of_teams,
            game_id=game_id,
            league_key=f"{game_id!s}.l.{league_id!s}",
        )
        set_end_points = determine_end_points(pipeline_params)

        db_params = DatabaseParameters(db_conn_uri=db_conn_uri, schema_name=None, table_name=None)

        end_point_param_list = []
        for end_point in set_end_points:
            if end_point == "get_player":
                for page_start in range(start_count, 2000, retrieval_limit):
                    end_point_param_list.append(
                        get_endpoint_config(
                            end_point=end_point,
                            page_start=page_start,
                            retrieval_limit=retrieval_limit,
                            player_key_list=None,
                        )
                    )

            elif end_point in ["get_player_draft_analysis", "get_player_stat", "get_player_pct_owned"]:
                player_key_list = get_player_key_list(db_params.db_conn_uri, pipeline_params.league_key)
                player_chunks = chunk_list_twenty_five(player_key_list)
                for chunked_player_list in player_chunks:
                    end_point_param_list.append(
                        get_endpoint_config(
                            end_point=end_point,
                            page_start=None,
                            retrieval_limit=None,
                            player_key_list=chunked_player_list,
                        )
                    )

            else:
                end_point_param_list.append(
                    get_endpoint_config(
                        end_point=end_point,
                        page_start=None,
                        retrieval_limit=None,
                        player_key_list=None,
                    )
                )
        return pipeline_params, db_params, end_point_param_list

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def extract_data(
    pipeline_params: PipelineParameters, end_point_params: EndPointParameters, yahoo_api: YahooAPI
) -> tuple[dict, YahooParseBase]:
    try:
        resp, data_parser = extractor(pipeline_params, end_point_params, yahoo_api)  # type: ignore
        return resp, data_parser

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def parse_data(data_parser: YahooParseBase, end_point_params: EndPointParameters) -> dict[str, DataFrame]:
    try:
        data = parse_response(data_parser, end_point_params.end_point)
        return data

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def load_raw_data(raw_data: dict, db_params: DatabaseParameters, columns: list[str] | None) -> bool:
    try:
        json_to_db(raw_data, db_params, columns)
        return True
    except Exception as e:
        raise e


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def load_parsed_data(resp_table_df: DataFrame, db_params: DatabaseParameters) -> bool:
    try:
        df_to_db(resp_table_df, db_params)
        return True
    except Exception as e:
        raise e


@flow(
    task_runner=SequentialTaskRunner(),
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
)
def load_pipeline_list(
    db_conn_uri: SecretStr,
    current_timestamp: datetime,
    game_id: int,
    league_id: int,
    num_of_teams: int,
) -> tuple[
    PipelineParameters,
    DatabaseParameters,
    tuple[list[EndPointParameters], list[EndPointParameters] | None, list[EndPointParameters] | None],
]:
    try:
        pipeline_params, db_params, end_point_list = get_parameters(
            db_conn_uri=db_conn_uri,
            current_timestamp=current_timestamp,
            game_id=game_id,
            league_id=league_id,
            num_of_teams=num_of_teams,
        )
        chunked_pipelines = split_pipelines(end_point_list=end_point_list)

        return pipeline_params, db_params, chunked_pipelines

    except Exception as e:
        raise e


@flow(
    task_runner=SequentialTaskRunner(),
    on_failure=[notify_discord_failure],
    on_cancellation=[notify_discord_cancellation],
)
def extract_transform_load(
    pipeline_params: PipelineParameters,
    db_params: DatabaseParameters,
    end_point_params: EndPointParameters,
    yahoo_api: YahooAPI,
) -> bool:
    try:
        resp, data_parser = extract_data(pipeline_params, end_point_params, yahoo_api)

        db_params.schema_name = "yahoo_json"
        db_params.table_name = end_point_params.end_point.replace("get_", "")
        load_raw = load_raw_data(raw_data=resp, db_params=db_params, columns=["yahoo_json"])  # noqa: F841

        db_params.schema_name = "yahoo_data"
        db_params.table_name = None
        parsed_data = parse_data(data_parser=data_parser, end_point_params=end_point_params)

        for table_name, table_df in parsed_data.items():
            db_params.table_name = table_name
            load_parsed_data(
                resp_table_df=table_df,
                db_params=db_params,
            )

        return True

    except Exception as e:
        raise e


@flow(on_failure=[notify_discord_failure], on_cancellation=[notify_discord_cancellation])
def yahoo_flow(
    current_timestamp: datetime,
    game_id: int = 423,
    league_id: int = 127732,
    num_of_teams: int = 10,
) -> bool:
    try:
        db_conn_uri = SecretStr(
            os.getenv("SUPABASE_CONN_PYTHON", "localhost")
            if ENV_STATUS == "local"
            else Secret.load("supabase-conn-python").get()  # type: ignore
        )
        pipeline_params, db_params, pipeline_chunks = load_pipeline_list(
            db_conn_uri=db_conn_uri,
            current_timestamp=current_timestamp,
            game_id=game_id,
            league_id=league_id,
            num_of_teams=num_of_teams,
        )

        pipelines = []
        if pipeline_chunks[1] and pipeline_chunks[2]:
            yahoo_config_list = get_yahoo_api_config(3)

            get_file_from_bucket(yahoo_config_list[0].token_file_path)  # type: ignore
            get_file_from_bucket(yahoo_config_list[1].token_file_path)  # type: ignore
            get_file_from_bucket(yahoo_config_list[2].token_file_path)  # type: ignore

            yahoo_api_one = YahooAPI(config=yahoo_config_list[0])  # type: ignore
            yahoo_api_two = YahooAPI(config=yahoo_config_list[1])  # type: ignore
            yahoo_api_three = YahooAPI(config=yahoo_config_list[2])  # type: ignore

            for chunk_one, chunk_two, chunk_three in zip(
                pipeline_chunks[0], pipeline_chunks[1], pipeline_chunks[2], strict=True
            ):
                pipe_one = extract_transform_load(pipeline_params, db_params, chunk_one, yahoo_api_one)  # type: ignore
                pipelines.append(pipe_one)

                pipe_two = extract_transform_load(pipeline_params, db_params, chunk_two, yahoo_api_two)  # type: ignore
                pipelines.append(pipe_two)

                pipe_three = extract_transform_load(pipeline_params, db_params, chunk_three, yahoo_api_three)  # type: ignore
                pipelines.append(pipe_three)

            upload_file_to_bucket(yahoo_config_list[0].token_file_path)  # type: ignore
            upload_file_to_bucket(yahoo_config_list[1].token_file_path)  # type: ignore
            upload_file_to_bucket(yahoo_config_list[2].token_file_path)  # type: ignore

        else:
            yahoo_config_list = get_yahoo_api_config(1)
            get_file_from_bucket(yahoo_config_list.token_file_path)  # type: ignore
            yahoo_api_one = YahooAPI(config=yahoo_config_list)  # type: ignore

            for chunk_one in pipeline_chunks[0]:
                pipe_one = extract_transform_load(pipeline_params, db_params, chunk_one, yahoo_api_one)  # type: ignore
                pipelines.append(pipe_one)

            upload_file_to_bucket(yahoo_config_list.token_file_path)  # type: ignore

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
    serve(sunday_flow, weekly_flow, off_pre_flow)  # type: ignore
