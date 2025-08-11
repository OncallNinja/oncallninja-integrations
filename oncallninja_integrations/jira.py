import requests
from typing import Dict, Any, Optional, List

from .action_router import action, ActionRouter

class JiraClient(ActionRouter):
    def __init__(self, instance_url: str, user_email: str, api_token: str):
        super().__init__()
        self.instance_url = instance_url
        self.auth = (user_email, api_token)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Any:
        """Helper method to make authenticated requests to the JIRA API."""
        url = f"{self.instance_url}{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=json_data,
                auth=self.auth
            )
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.RequestException as e:
            print(f"Error making JIRA API request to {url}: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise

    def _find_user_id(self, email: str) -> Optional[str]:
        """Find a JIRA user's accountId by their email."""
        if not email:
            return None
        
        try:
            users = self._make_request("GET", "/rest/api/3/user/search", params={"query": email})
            if users:
                return users[0].get("accountId")
        except Exception as e:
            print(f"Error finding user by email {email}: {e}")
        
        return None

    @action(description="Creates a JIRA issue and assigns it if an assignee email is provided.")
    def create_issue(self, project_key: str, summary: str, description: str, assignee_email: Optional[str] = None, issue_type: str = "Task") -> Dict[str, Any]:
        """Creates a JIRA issue and assigns it if an assignee email is provided."""
        endpoint = "/rest/api/3/issue"
        
        issue_data = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}]
                },
                "issuetype": {"name": issue_type},
            }
        }

        if assignee_email:
            assignee_id = self._find_user_id(assignee_email)
            if assignee_id:
                issue_data["fields"]["assignee"] = {"accountId": assignee_id}
            else:
                print(f"Could not find JIRA user with email: {assignee_email}. Issue will be created unassigned.")

        return self._make_request("POST", endpoint, json_data=issue_data)
    
def main():
    """
    Example of how to use the JiraClient for testing.
    
    To run this, set the following environment variables:
    - JIRA_INSTANCE_URL: Your JIRA instance URL (e.g., https://your-domain.atlassian.net)
    - JIRA_USER_EMAIL: The email of the user (or service account) to authenticate as.
    - JIRA_API_TOKEN: The API token for the user.
    """
    import os

    instance_url = os.getenv("JIRA_INSTANCE_URL")
    user_email = os.getenv("JIRA_USER_EMAIL")
    api_token =  os.getenv("JIRA_API_TOKEN")

    if not all([instance_url, user_email, api_token]):
        print("Please set JIRA_INSTANCE_URL, JIRA_USER_EMAIL, and JIRA_API_TOKEN environment variables.")
        return

    jira_client = JiraClient(
        instance_url=instance_url,
        user_email=user_email,
        api_token=api_token
    )

    try:
        print("Attempting to create a test issue...")
        issue = jira_client.create_issue(
            project_key="KAN",  # <-- IMPORTANT: Change to a valid Project Key (e.g., "PROJ"), not the full project name.
            summary="Test Issue from JiraClient",
            description="This is a test issue created from the main function in jira.py.",
            assignee_email=user_email  # Assigning to self for the test
        )
        print(f"Successfully created issue: {issue.get('key')}")
        print(f"URL: {instance_url}/browse/{issue.get('key')}")
    except Exception as e:
        print(f"Failed to create test issue: {e}")

if __name__ == "__main__":
    main()
