from prefect.blocks.notifications import DiscordWebhook
from prefect.client.schemas.objects import Flow, FlowRun, State
from prefect.settings import PREFECT_API_URL
from prefect_gcp import GcpCredentials, GcsBucket
from pydantic import SecretStr


def notify_discord_failure(flow: Flow, flow_run: FlowRun, state: State) -> None:
    discord_webhook_block = DiscordWebhook.load("mom-notifications")
    body = """
--------------------------------------------------------------------
# FAILED JOB:
## `{flow_name}`
### Tags:
- {flow_tags}
### INFO:
- **Scheduled start:**
    - {flow_start_time}
- **Total run time:**
    - {total_run_time}
- **Access URL:**
    - {prefect_url}/flow-runs/flow-run/{flow_id}
- **State Name:**
    - {state_name}
- **State Message:**
    - {state_message}
- **Deployment ID:**
    - {deployment_id}
- **Parent Task ID:**
    - {parent_task_run_id}
### Parameters:
- {flow_parameters}
"""
    conceal_list = [
        "db_conn_uri",
        "consumer_secret",
        "consumer_key",
        "tokey_file_path",
        "yahoo_consumer_key",
        "yahoo_consumer_secret",
    ]
    parameters = [
        f"{k}: `{v}`\n" if k not in conceal_list else f"{k}: `{SecretStr(v)}`\n" for k, v in flow_run.parameters.items()
    ]
    discord_webhook_block.notify(  # type: ignore
        body.format(
            flow_name=flow.name,
            flow_tags="- ".join(f"{tag}\n" for tag in flow_run.tags) if flow_run.tags else "None",
            flow_start_time=flow_run.expected_start_time,
            total_run_time=flow_run.total_run_time,
            prefect_url=(
                PREFECT_API_URL.value().replace("https//api", "https://app").replace("api/accounts", "accounts")
            ),
            flow_id=flow_run.id,
            state_name=state.name,
            state_type=state.type,
            state_data=state.data,
            state_message=state.message,
            deployment_id=flow_run.deployment_id,
            parent_task_run_id=flow_run.parent_task_run_id,
            flow_parameters="- ".join(parameters),
        )
    )


def notify_discord_cancellation(flow: Flow, flow_run: FlowRun, state: State) -> None:
    discord_webhook_block = DiscordWebhook.load("mom-notifications")
    body = """
--------------------------------------------------------------------
# CANCELLED JOB:
## `{flow_name}`
### Tags:
- {flow_tags}
### INFO:
- **Scheduled start:**
    - {flow_start_time}
- **Total run time:**
    - {total_run_time}
- **Access URL:**
    - https://{prefect_url}/flow-runs/flow-run/{flow_id}
- **State Name:**
    - {state_name}
- **State Message:**
    - {state_message}
- **Deployment ID:**
    - {deployment_id}
- **Parent Task ID:**
    - {parent_task_run_id}
### Parameters:
- {flow_parameters}
"""
    conceal_list = [
        "db_conn_uri",
        "consumer_secret",
        "consumer_key",
        "tokey_file_path",
        "yahoo_consumer_key",
        "yahoo_consumer_secret",
    ]
    parameters = [
        f"{k}: `{v}`\n" if k not in conceal_list else f"{k}: `{SecretStr(v)}`\n" for k, v in flow_run.parameters.items()
    ]
    discord_webhook_block.notify(  # type: ignore
        body.format(
            flow_name=flow.name,
            flow_tags="- ".join(f"{tag}\n" for tag in flow_run.tags) if flow_run.tags else "None",
            flow_start_time=flow_run.expected_start_time,
            total_run_time=flow_run.total_run_time,
            prefect_url=PREFECT_API_URL.value(),
            flow_id=flow_run.id,
            state_name=state.name,
            state_type=state.type,
            state_data=state.data,
            state_message=state.message,
            deployment_id=flow_run.deployment_id,
            parent_task_run_id=flow_run.parent_task_run_id,
            flow_parameters="- ".join(parameters),
        )
    )


def get_file_from_bucket(file: str) -> None:
    gcp_credentials = GcpCredentials.load("google-storage-credentials")
    gcs_bucket = GcsBucket(bucket="men-of-madison", gcp_credentials=gcp_credentials)
    gcs_bucket.download_object_to_path(file, file)  # type: ignore


def upload_file_to_bucket(file: str) -> None:
    gcp_credentials = GcpCredentials.load("google-storage-credentials")
    gcs_bucket = GcsBucket(bucket="men-of-madison", gcp_credentials=gcp_credentials)
    gcs_bucket.upload_from_path(file, file)  # type: ignore
