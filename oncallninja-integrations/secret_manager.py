from google.cloud import secretmanager_v1 as secretmanager
import os
import google.auth

class SecretManager:
    """A class for fetching secrets from Google Cloud Secret Manager"""

    def __init__(self, project_id=None):
        """
        Initialize the SecretManager client.

        Args:
            project_id (str, optional): GCP project ID. If None, will attempt to get from environment.
        """
        # If project_id is not provided, try to get it from the environment
        if project_id is None:
            # In Cloud Run, this environment variable is typically available
            project_id = os.environ.get('PROJECT_ID')

            if project_id is None:
                try:
                    _, project_id = google.auth.default()
                    print(f"Auto-detected project ID: {project_id}")
                except Exception as e:
                    print(f"Could not auto-detect project ID: {e}")

        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    def get_secret(self, secret_id, version_id="latest"):
        """
        Fetch a secret from Secret Manager.

        Args:
            secret_id (str): The ID of the secret to fetch
            version_id (str, optional): The version of the secret to fetch. Defaults to "latest".

        Returns:
            str: The secret value as a string

        Raises:
            Exception: If the secret cannot be accessed
        """
        # Build the resource name of the secret version
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"

        try:
            # Access the secret version using the current API
            response = self.client.access_secret_version(name=name)

            # Return the decoded payload
            return response.payload.data.decode('UTF-8')
        except Exception as e:
            print(f"Secret doesn't exist {secret_id}: {e}")
            return None

    def load_integration_secrets(self):
        """
        Load all integration secrets and return them as a dictionary.

        Returns:
            dict: Dictionary containing all integration secrets
        """
        secrets = {}

        # Define all possible secrets
        secret_names = [
            "bitbucket-api-key",
            "launchdarkly-api-key",
            "launchdarkly-sdk-key",
            "kibana-cloud-id",
            "kibana-api-key",
            "slack-bot-token",
            "slack-signing-secret"
        ]

        # Try to load each secret
        for secret_name in secret_names:
            try:
                secrets[secret_name] = self.get_secret(secret_name)
                print(secrets)
            except Exception:
                # If the secret doesn't exist or can't be accessed, just skip it
                print(f"Secret {secret_name} not found or not accessible")
                continue

        return secrets


# Example usage
if __name__ == "__main__":
    try:
        # Create the Secret Manager client
        sm = SecretManager()

        # Example of fetching a specific secret
        bitbucket_api_key = sm.get_secret("bitbucket-api-key")
        print(f"Successfully retrieved BitBucket API key: {bitbucket_api_key[:3]}...")

        # Load all integration secrets
        secrets = sm.load_integration_secrets()
        print(f"Loaded {len(secrets)} secrets")

    except Exception as e:
        print(f"Error: {e}")