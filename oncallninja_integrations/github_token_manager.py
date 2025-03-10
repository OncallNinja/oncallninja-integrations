import json
import time
import requests
import logging
from datetime import datetime, timedelta
import subprocess
from google.cloud import secretmanager


class GitHubTokenManager:
    """
    Manages GitHub personal access tokens with automatic refresh capability,
    storing token information in Google Secret Manager.
    """

    def __init__(self, secret_id, project_id, scopes=None, note=None):
        """
        Initialize the token manager.

        Args:
            secret_id (str): ID for the secret in Google Secret Manager
            project_id (str): Google Cloud project ID
            scopes (list): List of scopes required for the token
            note (str): A note to identify this token in GitHub
        """
        self.secret_id = secret_id
        self.project_id = project_id
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.scopes = scopes or ["repo"]
        self.note = note or "Auto-refreshed token for API access"
        self.headers = None
        self.load_token()

    def get_secret_name(self):
        """Construct the full secret name for Google Secret Manager."""
        return f"projects/{self.project_id}/secrets/{self.secret_id}"

    def get_secret_version_name(self):
        """Construct the latest version name for the secret."""
        return f"{self.get_secret_name()}/versions/latest"

    def secret_exists(self):
        """Check if the secret exists in Google Secret Manager."""
        try:
            self.secret_client.get_secret(name=self.get_secret_name())
            return True
        except Exception:
            return False

    def create_secret_if_needed(self):
        """Create the secret if it doesn't exist."""
        if not self.secret_exists():
            parent = f"projects/{self.project_id}"
            self.secret_client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": self.secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )

    def load_token(self):
        """Load token from Google Secret Manager if it exists and is valid."""
        try:
            if self.secret_exists():
                secret_version = self.secret_client.access_secret_version(
                    request={"name": self.get_secret_version_name()}
                )

                token_data = json.loads(secret_version.payload.data.decode("UTF-8"))

                # Check if token is close to expiration (1 day buffer)
                expiry = datetime.fromisoformat(token_data['expiry'])
                if datetime.now() + timedelta(days=1) < expiry:
                    self.token = token_data['token']
                    self.expiry = expiry
                    self.headers = {
                        "Authorization": f"Bearer {self.token}",
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28"
                    }
                    logging.info("Loaded existing token valid until: %s", expiry.strftime("%Y-%m-%d %H:%M:%S"))
                    return
                else:
                    logging.info("Token is expiring soon, refreshing")
            else:
                logging.info("No token found in Secret Manager, creating new token")
        except Exception as e:
            logging.warning("Error loading token from Secret Manager: %s", str(e))

        # If we get here, we need a new token
        self.refresh_token()

    def refresh_token(self):
        """Generate a new token using GitHub API."""
        try:
            # This function needs to be implemented to generate a new GitHub token
            # Since we're in a cloud environment, we'll need to use a programmatic approach
            # One approach is to use a GitHub App installation token
            # Another is to use a user-to-server OAuth token
            # For simplicity in this example, I'll show a basic service account approach

            # NOTE: In a real implementation, you'd replace this with a proper token generation method
            # that works without requiring interactive auth or CLI tools

            # Generate new token via GitHub API (this is a placeholder)
            # In a real implementation, you'd use one of these methods:
            # 1. GitHub App installation tokens
            # 2. OAuth device flow if some user interaction is acceptable during deployment
            # 3. Use a provisioned service account token

            # For demonstration purposes, we'll simulate getting a token
            # In reality, you'll need to replace this with actual API calls to GitHub
            self._simulate_token_generation()

            # Save the token to Secret Manager
            self._save_token_to_secret_manager()

            logging.info("Generated new token valid until: %s", self.expiry.strftime("%Y-%m-%d %H:%M:%S"))

        except Exception as e:
            logging.error("Failed to refresh token: %s", str(e))
            raise RuntimeError(f"Failed to refresh GitHub token: {str(e)}")

    def _simulate_token_generation(self):
        """
        PLACEHOLDER: This method simulates token generation.
        In a real implementation, you would replace this with actual GitHub API calls.
        """
        # This is where you'd implement your actual token generation logic
        # For now, we're just simulating a token for demonstration

        # In a real implementation, you'd use one of these methods:
        # 1. GitHub App installation tokens (recommended)
        # 2. GitHub OAuth device flow
        # 3. GitHub personal access token with webhook notification before expiry

        # Example implementation with GitHub App (pseudocode):
        # jwt = create_jwt_for_github_app(private_key, app_id)
        # installation_id = get_installation_id(jwt, organization)
        # token_response = get_installation_token(jwt, installation_id)
        # new_token = token_response['token']
        # expires_at = token_response['expires_at']

        # For this example, we'll just pretend we got a token
        import uuid
        self.token = f"github_pat_{uuid.uuid4().hex}"
        self.expiry = datetime.now() + timedelta(days=7)

        # Update headers
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

    def _save_token_to_secret_manager(self):
        """Save the token to Google Secret Manager."""
        token_data = {
            'token': self.token,
            'expiry': self.expiry.isoformat(),
            'scopes': self.scopes
        }

        # Convert token data to JSON string
        token_data_str = json.dumps(token_data)

        # Create the secret if it doesn't exist
        self.create_secret_if_needed()

        # Add the new version
        parent = self.get_secret_name()
        self.secret_client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": token_data_str.encode("UTF-8")},
            }
        )

    def get_headers(self):
        """Get authorization headers, refreshing token if needed."""
        # Check if token is close to expiration (1 day buffer)
        if not hasattr(self, 'expiry') or datetime.now() + timedelta(days=1) >= self.expiry:
            self.refresh_token()
        return self.headers

    def make_request(self, method, url, **kwargs):
        """Make a GitHub API request with automatic token refresh."""
        if not url.startswith('https://'):
            url = f"https://api.github.com{url}"

        # Ensure we have valid headers
        headers = self.get_headers()
        if 'headers' in kwargs:
            # Merge provided headers with auth headers
            kwargs['headers'] = {**kwargs['headers'], **headers}
        else:
            kwargs['headers'] = headers

        response = requests.request(method, url, **kwargs)

        # Handle 401/403 by trying to refresh token once
        if response.status_code in (401, 403) and 'token' in response.text.lower():
            logging.info("Token appears invalid, attempting refresh")
            self.refresh_token()

            # Update headers with new token
            if 'headers' in kwargs:
                kwargs['headers'] = {**kwargs['headers'], **self.headers}
            else:
                kwargs['headers'] = self.headers

            # Retry the request
            response = requests.request(method, url, **kwargs)

        return response


class GitHubTokenManagerWithGitHubApp(GitHubTokenManager):
    """
    Enhanced token manager that uses GitHub App installation tokens.
    This is a more realistic implementation for Cloud Run.
    """

    def __init__(self, secret_id, project_id, app_id, private_key_secret_id, installation_id):
        """
        Initialize the token manager.

        Args:
            secret_id (str): ID for the token secret in Google Secret Manager
            project_id (str): Google Cloud project ID
            app_id (int): GitHub App ID
            private_key_secret_id (str): Secret ID for the GitHub App private key
            installation_id (int): GitHub App installation ID
        """
        self.app_id = app_id
        self.private_key_secret_id = private_key_secret_id
        self.installation_id = installation_id
        super().__init__(secret_id, project_id)

    def _get_private_key(self):
        """Get the GitHub App private key from Secret Manager."""
        private_key_name = f"projects/{self.project_id}/secrets/{self.private_key_secret_id}/versions/latest"
        response = self.secret_client.access_secret_version(request={"name": private_key_name})
        return response.payload.data.decode("UTF-8")

    def _create_jwt(self):
        """Create a JWT for GitHub App authentication."""
        import jwt
        import time

        # Get the private key from Secret Manager
        private_key = self._get_private_key()

        # Current time and expiration time (10 minutes from now)
        now = int(time.time())
        expiration = now + (10 * 60)

        # Create the JWT payload
        payload = {
            "iat": now,
            "exp": expiration,
            "iss": self.app_id
        }

        # Create the JWT
        token = jwt.encode(payload, private_key, algorithm="RS256")

        return token

    def _get_installation_token(self):
        """Get an installation token for the GitHub App."""
        jwt_token = self._create_jwt()

        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

        url = f"https://api.github.com/app/installations/{self.installation_id}/access_tokens"
        response = requests.post(url, headers=headers)

        if response.status_code != 201:
            raise RuntimeError(f"Failed to get installation token: {response.text}")

        token_data = response.json()
        return token_data

    def _simulate_token_generation(self):
        """
        Get a real GitHub App installation token.
        This replaces the simulation in the parent class.
        """
        # Get an installation token
        token_data = self._get_installation_token()

        # Parse the token and expiry
        self.token = token_data["token"]

        # GitHub installation tokens expire after 1 hour by default
        # The expiry time is returned in the response
        self.expiry = datetime.fromisoformat(token_data["expires_at"].replace("Z", "+00:00"))

        # Update headers
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }


# Example usage for Cloud Run
class GitHubRepo:
    def __init__(self, project_id):
        # For GitHub App implementation (recommended)
        self.token_manager = GitHubTokenManagerWithGitHubApp(
            secret_id="github-token-secret",
            project_id=project_id,
            app_id=123456,  # Replace with your GitHub App ID
            private_key_secret_id="github-app-private-key",
            installation_id=987654  # Replace with your Installation ID
        )

        # Alternative: using the base implementation
        # self.token_manager = GitHubTokenManager(
        #     secret_id="github-token-secret",
        #     project_id=project_id
        # )

        self.base_url = "https://api.github.com"

    def get_repository(self, repo_path):
        """Get repository information."""
        return self.token_manager.make_request(
            "GET",
            f"{self.base_url}/repos/{repo_path}",
        )

    def get_repository_contents(self, repo_path, file_path=None):
        """Get repository contents."""
        url = f"{self.base_url}/repos/{repo_path}/contents"
        if file_path:
            url = f"{url}/{file_path}"
        return self.token_manager.make_request("GET", url)


# Example of how to use it in Cloud Run
def main(request):
    """Cloud Run entry point."""
    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Get project ID from metadata server (works in GCP)
    # In Cloud Run, you can also set this as an environment variable
    project_id = "your-gcp-project-id"  # Replace with your GCP project ID

    # Create repo client
    client = GitHubRepo(project_id)

    # Example API call
    try:
        response = client.get_repository("horus-ai-labs/DistillFlow")

        if response.status_code == 200:
            repo_data = response.json()
            return f"Successfully accessed repository: {repo_data['full_name']}"
        else:
            error_message = f"Error: {response.status_code} - {response.text}"
            logging.error(error_message)
            return error_message, 500
    except Exception as e:
        logging.exception("Failed to access repository")
        return f"Error: {str(e)}", 500