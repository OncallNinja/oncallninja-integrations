import logging
import ssl

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta

from integrations.action_router import ActionRouter, action
from redactor.redact import redact_text, redact_message_blocks


class SlackClient(ActionRouter):
    def __init__(self, slack_token: str):
        """
        Initialize the LaunchDarkly integration with your API key.

        Args:
            api_key: LaunchDarkly REST API key with appropriate permissions
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)

        # Create a more permissive SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Slack client initialization with custom SSL handling
        self.slack_client = WebClient(
            token=slack_token,
            ssl=ssl_context
        )

    @action(description="Fetch all channels")
    def get_all_channels(self):
        result = self.slack_client.conversations_list(
            types="public_channel,private_channel",
            limit=1000
        )

        all_channels = []
        for channel in result['channels']:
            all_channels.append({channel['name']: channel['id']})

        self.logger.debug(f"Available channels {all_channels}")
        return all_channels

    def _redact(self, message):
        text = message.get("text")
        redacted_text = redact_text(text)['redacted_text']
        message["text"] = redacted_text
        blocks = message.get('blocks', '')
        if blocks:
            redacted_blocks = redact_message_blocks(blocks)
            message["blocks"] = redacted_blocks
        return message

    @action(description="Get messages from a channel between given start and end time")
    def get_messages_for_channel(self, channel_id, start_time, end_time):
        """
        Retrieve messages for a specific channel within a time range

        :param channel_id: Slack channel ID
        :param start_time: Start timestamp
        :param end_time: End timestamp
        :return: Generator of messages
        """
        try:
            # Fetch conversations history
            result = self.slack_client.conversations_history(
                channel=channel_id,
                oldest=str(start_time),
                latest=str(end_time),
                limit=1000  # Max per request
            )
            print(f"result: {result}")

            for message in result['messages']:
                yield self._redact(message)

                # Check if the message has a thread
                if 'thread_ts' in message or message.get('reply_count', 0) > 0:
                    # Fetch and yield thread replies
                    thread_ts = message['ts']
                    thread_replies = self.get_thread_replies(channel_id, thread_ts)

                    for reply in thread_replies:
                        # Add a flag to indicate it's a thread reply
                        reply['is_thread_reply'] = True
                        reply['parent_message_ts'] = thread_ts
                        yield self._redact(reply)

        except SlackApiError as e:
            self.logger.error(f"Error fetching messages: {e}")

    @action(description="Fetch all messages from within a thread")
    def get_thread_replies(self, channel_id, thread_ts):
        """
        Retrieve all replies in a thread

        :param channel_id: Channel ID
        :param thread_ts: Timestamp of the parent message
        :return: List of thread replies
        """
        try:
            # Fetch thread replies
            result = self.slack_client.conversations_replies(
                channel=channel_id,
                ts=thread_ts
            )

            # Return all messages in the thread, excluding the parent message
            return result['messages'][1:]

        except SlackApiError as e:
            self.logger.error(f"Error fetching thread replies: {e}")
            return []

    @action(description="Fetch messages from a given channel Id")
    def process_channels(self, channel_ids, lookback_days=1):
        """
        Process messages across multiple channels

        :param channel_ids: List of channel IDs to process
        :param lookback_days: Number of days to look back
        """
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(days=lookback_days)

        # Convert to timestamps
        start_timestamp = start_time.timestamp()
        end_timestamp = end_time.timestamp()

        messages = []
        # Process each channel
        for channel_id in channel_ids:
            self.logger.info(f"Processing channel: {channel_id}")

            for message in self.get_messages_for_channel(channel_id, start_timestamp, end_timestamp):
                messages.append(message)
        return messages

