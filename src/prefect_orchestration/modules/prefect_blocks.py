from typing import Any

from prefect.blocks.core import Block
from prefect.blocks.notifications import DiscordWebhook
from prefect.settings import PREFECT_API_URL
from prefect_gcp import GcpCredentials, GcsBucket
from pydantic import SecretStr
from yahoo_export import Config, YahooAPI


class YahooConfigBlock(Block):
    consumer_key: SecretStr
    consumer_secret: SecretStr
    current_nfl_week: int
    current_nfl_season: int
    league_info: dict[str, Any]
    token_file_path: str

    def get_yahoo_session(self) -> YahooAPI:
        config = Config(
            yahoo_consumer_key=self.consumer_key,
            yahoo_consumer_secret=self.consumer_secret,
            current_nfl_week=self.current_nfl_week,
            current_nfl_season=self.current_nfl_season,
            league_info=self.league_info,
            token_file_path=self.token_file_path,
        )
        return YahooAPI(config=config)


def notify_discord(flow, flow_run, state):
    discord_webhook_block = DiscordWebhook.load("mom-notifications")
    body = """
# @cudde2

## JOB: {flow_name} failed

### Tags:{flow_tags}

### INFO:

- Scheduled start: {flow_start_time}
- Total run time: {total_run_time}
- https://{prefect_url_1}/flow-runs
- https://{prefect_url_2}/flow-run/{flow_id_1}
- flow-run/{flow_id_2} | the flow run in the UI>
- State Name: {state_name}
- State Type: {state_id}
- State Data: {state_data}
- Deployment ID: {deployment_id}
- Parent Task ID: {parent_task_run_id}

### Parameters:{flow_parameters}
"""
    discord_webhook_block.notify(
        body.format(
            flow_name=flow.name,
            flow_tags="\n- ".join(flow.tags),
            flow_start_time=flow_run.expected_start_time,
            total_run_time=flow_run.total_run_time,
            prefect_url_1=PREFECT_API_URL.value(),
            prefect_url_2=PREFECT_API_URL.value(),
            flow_id_1=flow_run.id,
            flow_id_2=flow_run.id,
            state_name=state.name,
            state_type=state.type,
            state_data=state.data,
            deployment_id=flow_run.deployment_id,
            parent_task_run_id=flow_run.parent_task_run_id,
            flow_parameters="\n- ".join(f"{k}: {v}" for k, v in flow_run.parameters.items()),
        )
    )


def get_file_from_bucket(file: str) -> None:
    gcp_credentials = GcpCredentials.load("google-storage-credentials")
    gcs_bucket = GcsBucket(bucket="men-of-madison", gcp_credentials=gcp_credentials)
    gcs_bucket.download_object_to_path(file, file)


def upload_file_to_bucket(file: str) -> None:
    gcp_credentials = GcpCredentials.load("google-storage-credentials")
    gcs_bucket = GcsBucket(bucket="men-of-madison", gcp_credentials=gcp_credentials)
    gcs_bucket.upload_from_path(file, file)
