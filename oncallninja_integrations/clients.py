from typing import Any

from .bitbucket import BitbucketClient, BitbucketConfig
from .kibana import KibanaClient
from .launchdarkly import LaunchDarklyClient
from .secret_manager import SecretManager
from .slack import SlackClient

secret_manager = SecretManager()

def get_launchdarkly_client():
    api_key = secret_manager.get_secret("launchdarkly-api-key")
    sdk_key = secret_manager.get_secret("launchdarkly-sdk-key")
    if api_key and sdk_key:
        return LaunchDarklyClient(api_key, sdk_key)
    return None

def get_kibana_client():
    cloud_id = secret_manager.get_secret("kibana-cloud-id")
    api_key = secret_manager.get_secret("kibana-api-key")
    if cloud_id and api_key:
        return KibanaClient(cloud_id=cloud_id, api_key=api_key)
    return None

def get_bitbucket_client():
    api_key = secret_manager.get_secret("bitbucket-api-key")
    if api_key:
        return BitbucketClient(BitbucketConfig(access_token=api_key))
    return None

def get_slack_client(redact_text = None, redact_message_blocks = None):
    slack_token = secret_manager.get_secret("slack-bot-token")
    if slack_token:
        return SlackClient(slack_token, redact_text = None, redact_message_blocks = None)
    return None

def fetch_resource(resource_type: str, action: str, params: dict[str: Any], redact_text = None, redact_message_blocks = None):
    """Fetch resource from connected services"""
    bitbucket_client = get_bitbucket_client()
    launchdarkly_client = get_launchdarkly_client()
    kibana_client = get_kibana_client()
    slack_client = get_slack_client(redact_text, redact_message_blocks)

    if resource_type == "code" and bitbucket_client:
        return bitbucket_client.execute_action(action, params)
    elif resource_type == "featureflag" and launchdarkly_client:
        return launchdarkly_client.execute_action(action, params)
    elif resource_type == "logs" and kibana_client:
        return kibana_client.execute_action(action, params)
    elif resource_type == "slack" and slack_client:
        return slack_client.execute_action(action, params)
    else:
        print(f"Requested unsupported resource type {resource_type}")
        raise ValueError(f"Unsupported resource type {resource_type}")

def get_actions():
    all_actions = []
    bitbucket_client = get_bitbucket_client()
    launchdarkly_client = get_launchdarkly_client()
    kibana_client = get_kibana_client()
    if bitbucket_client:
        all_actions.append({"code": bitbucket_client.available_actions()})
    if launchdarkly_client is not None:
        all_actions.append({"featureflag": launchdarkly_client.available_actions()})
    if kibana_client is not None:
        all_actions.append({"logs": kibana_client.available_actions()})

    return all_actions
