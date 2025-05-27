import logging
import ssl

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta

from .action_router import ActionRouter, action

class SlackClient(ActionRouter):
    def __init__(self, slack_token: str, redact_text = None, redact_message_blocks = None):
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
        self.redact_text = redact_text
        self.redact_message_blocks = redact_message_blocks

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
        if not self.redact_text:
            return message

        text = message.get("text")
        redacted_text = self.redact_text(text)
        message["text"] = redacted_text
        blocks = message.get('blocks', '')
        if blocks:
            redacted_blocks = self.redact_message_blocks(blocks)
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

    @action(description="Fetch a single conversation thread using the channel ID and thread TS")
    def fetch_conversation(self, channel_id: str, thread_ts: str):
        try:
            # First, get the parent message
            result = self.slack_client.conversations_history(
                channel=channel_id,
                oldest=thread_ts,
                latest=thread_ts,
                inclusive=True,
                limit=1
            )

            if not result['messages']:
                return {"error": "Message not found"}

            parent_message = self._redact(result['messages'][0])

            # Then get thread replies if it's a thread
            thread_replies = []
            if parent_message.get('thread_ts') or parent_message.get('reply_count', 0) > 0:
                thread_replies = self.get_thread_replies(channel_id, thread_ts)
                thread_replies = [self._redact(reply) for reply in thread_replies]

            return {
                "parent_message": parent_message,
                "thread_replies": thread_replies,
                "channel_id": channel_id,
                "thread_ts": thread_ts
            }

        except SlackApiError as e:
            self.logger.error(f"Error fetching conversation: {e}")
            return {"error": str(e)}

    @action(description="Fetch a single conversation thread using a Slack URL")
    def fetch_conversation_from_url(self, slack_url):
        """
        Parse a Slack URL and fetch the conversation thread

        :param slack_url: URL to a Slack message (e.g., https://team.slack.com/archives/C01234ABCD/p1234567890123456)
        :return: Dictionary containing the parent message and all thread replies
        """
        try:
            # Parse the URL to extract channel ID and timestamp
            url_parts = slack_url.split('/')

            # Find the channel ID (typically starts with C or G)
            channel_id = None
            timestamp = None

            for i, part in enumerate(url_parts):
                if part == 'archives' and i + 1 < len(url_parts):
                    channel_id = url_parts[i + 1]
                elif part.startswith('p') and i > 0 and len(part) > 1:
                    # Extract timestamp from the p1234567890123456 format
                    ts_str = part[1:]  # Remove the 'p'

                    # Slack timestamps in URLs have 16+ digits but the API expects
                    # a format like "1234567890.123456"
                    if len(ts_str) >= 16:
                        # Insert the decimal point at the right position
                        timestamp = f"{ts_str[:-6]}.{ts_str[-6:]}"

            if not channel_id or not timestamp:
                raise ValueError(f"Could not parse channel ID or timestamp from URL: {slack_url}")

            self.logger.info(f"Parsed URL: channel_id={channel_id}, timestamp={timestamp}")
            return self.fetch_conversation(channel_id, timestamp)
        except ValueError as e:
            self.logger.error(f"URL parsing error: {e}")
            return {"error": str(e)}
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return {"error": f"Unexpected error: {str(e)}"}