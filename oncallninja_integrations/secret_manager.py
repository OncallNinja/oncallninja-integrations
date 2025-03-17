from google.cloud import secretmanager_v1 as secretmanager
import os
import google.auth
import yaml


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
            except Exception:
                # If the secret doesn't exist or can't be accessed, just skip it
                print(f"Secret {secret_name} not found or not accessible")
                continue

        return secrets


    def save_yaml_as_secret(self, secret_id, yaml_file_path):
        """
        Reads a YAML file and saves its contents as a secret to Google Secret Manager.

        Args:
            project_id (str): Your Google Cloud project ID
            secret_id (str): The ID for the secret
            yaml_file_path (str): Path to the YAML file to store as a secret

        Returns:
            str: Name of the created secret version
        """
        # Build the resource name of the parent project
        # Read the YAML file
        try:
            with open(yaml_file_path, 'r') as file:
                # Load and then dump the YAML to ensure it's valid
                yaml_content = yaml.safe_load(file)
                secret_value = yaml.dump(yaml_content)
                print(f"Successfully loaded YAML from {yaml_file_path}")
        except Exception as e:
            print(f"Error reading YAML file: {e}")
            return None

        # Check if the secret already exists
        try:
            secret = self.get_secret(secret_id)
            if secret is not None:
                print(f"Secret {secret_id} already exists")
            else:
                print(f"Creating new secret {secret_id}")
                secret = self.client.create_secret(
                    request={
                        "parent": f"projects/{self.project_id}",
                        "secret_id": secret_id,
                        "secret": {"replication": {"automatic": {}}},
                    }
                )
        except Exception:
            # Create the secret
            print(f"Creating new secret {secret_id}")
            secret = self.client.create_secret(
                request={
                    "parent": f"projects/{self.project_id}",
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )

        # Build the resource name of the secret
        secret_path = f"projects/{self.project_id}/secrets/{secret_id}"

        # Add a new secret version
        version = self.client.add_secret_version(
            request={
                "parent": secret_path,
                "payload": {"data": secret_value.encode("UTF-8")},
            }
        )

        print(f"Added secret version: {version.name}")
        return version.name


    def save_secret(self, project_id, secret_id, secret_value):
        """
        Saves a secret to Google Secret Manager.

        Args:
            project_id (str): Your Google Cloud project ID
            secret_id (str): The ID for the secret
            secret_value (str): The value of the secret to store

        Returns:
            str: Name of the created secret version
        """

        # Build the resource name of the parent project
        parent = f"projects/{project_id}"

        # Check if the secret already exists
        try:
            secret = self.get_secret({secret_id})
            print(f"Secret {secret_id} already exists")
            if secret is not None:
                print(f"Secret {secret_id} already exists")
            else:
                print(f"Creating new secret {secret_id}")
                secret = self.client.create_secret(
                    request={
                        "parent": f"projects/{self.project_id}",
                        "secret_id": secret_id,
                        "secret": {"replication": {"automatic": {}}},
                    }
                )
        except Exception:
            # Create the secret
            print(f"Creating new secret {secret_id}")
            secret = self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )

        # Build the resource name of the secret
        secret_path = f"{parent}/secrets/{secret_id}"

        # Add a new secret version
        version = self.client.add_secret_version(
            request={
                "parent": secret_path,
                "payload": {"data": secret_value.encode("UTF-8")},
            }
        )

        print(f"Added secret version: {version.name}")
        return version.name


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
