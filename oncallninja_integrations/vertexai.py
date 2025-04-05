import json
from google.cloud import secretmanager
from google.oauth2 import service_account
from anthropic import AnthropicVertex

class SonnetModel:
    def __init__(self, project_id: str = 'kenjutsu', model_name: str = "claude-3-5-sonnet-v2@20241022"):
        """
        Initialize the Sonnet model using the Vertex AI API with credentials from Secret Manager.

        Args:
            project_id: Your Google Cloud project ID (default: 'kenjutsu').
            model_name: The name of the Sonnet model to use (default: "claude-3-5-sonnet-v2@20241022").
        """
        try:
            # Initialize Secret Manager client
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id}/secrets/vertexai-claude-3-5/versions/latest"
            
            # Access the secret
            response = secret_client.access_secret_version(name=secret_name)
            service_account_info = json.loads(response.payload.data.decode('UTF-8'))
            
            # Create credentials object with explicit scopes
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            credentials = credentials.with_quota_project(project_id)

            # Initialize the client with explicit credentials
            location = 'us-east5'
            self.client = AnthropicVertex(
                region=location,
                project_id=project_id,
                credentials=credentials
            )
            self.model_name = model_name
            
        except Exception as e:
            raise RuntimeError(f"Failed to initialize SonnetModel: {str(e)}")
    def invoke(self, string: str = None, prompt: dict = None):
        """
        Invoke the Sonnet model with a given input string.

        Args:
            string: The input string to send to the model.
            prompt: prompt from outside
        Returns:
            The complete response as a string.
        """
        if not prompt:
            message = {
                "role": "user",
                "content": string
            }
            message = [message]

            response = self.client.messages.create(
                max_tokens=1024,
                messages=message,
                model=self.model_name,
            )
        else:
            response = self.client.messages.create(**prompt)

        return response
