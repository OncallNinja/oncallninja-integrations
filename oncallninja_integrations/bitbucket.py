import base64
import fnmatch
import logging
import os
import subprocess

import requests
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from .action_router import action
from .code_client import CodingClient

class BitbucketConfig(BaseModel):
    access_token: str = Field(..., description="Bitbucket access token")
    api_url: str = Field("https://api.bitbucket.org/2.0", description="Bitbucket API URL")
    work_dir: str = Field("/tmp/oncallninja-repos", description="Working directory for cloning repos")
    max_commits_to_analyze: int = Field(10, description="Maximum number of recent commits to analyze")

class BitbucketClient(CodingClient):
    def __init__(self, config: BitbucketConfig):
        super().__init__(config.work_dir)
        self.logger = logging.getLogger(__name__)
        self.config = config
        # Bitbucket uses Basic Auth with username and app password
        self.headers = {
            "Authorization": f"Bearer {config.access_token}",
            "Accept": "application/json"
        }
        os.makedirs(self.config.work_dir, exist_ok=True)

    @action(description="Make a request to the Bitbucket API endpoint with the given params and data")
    def _make_request(self, endpoint: str, params: Dict = None) -> Any:
        """
        Make a request to the Bitbucket API.

        Args:
            endpoint: API endpoint to call (without base URL)
            params: Query parameters
            method: HTTP method (GET, POST, PUT, DELETE)

        Returns:
            Response JSON as dictionary
        """

        url = endpoint if self.config.api_url in endpoint else f"{self.config.api_url}{endpoint}"

        try:
            response = requests.get(
                url=url,
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error making request to Bitbucket API: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.logger.error(f"Response content: {e.response.text}")
            raise

    @action(description="Lists all accessible workspaces")
    def list_workspaces(self) -> List[str]:
        """List all accessible repositories."""

        # Bitbucket API uses pagination
        params = {"pagelen": 100}  # Maximum allowed per page
        all_workspaces = set()

        endpoint = "/repositories?role=member"
        while True:
            response = self._make_request(endpoint, params=params)
            values = response.get("values", [])

            for repo in values:
                if repo.get("is_private") and not repo.get("has_access", True):
                    continue
                all_workspaces.add(repo.get("full_name").split("/")[0])

            # Handle pagination
            next_page = response.get("next")
            if not next_page:
                break

            # For next page, we use the full URL
            params = None
            endpoint = next_page

        return list(all_workspaces)

    @action(description="Lists all accessible repositories, filter repositories by passing specific workspace")
    def list_repositories(self, filter_workspace: Optional[str]) -> List[Dict[str, Any]]:
        """List all accessible repositories."""
        if not filter_workspace:
            # Get user's repositories
            endpoint = "/repositories?role=member"
        else:
            endpoint = f"/repositories/{filter_workspace}"

        # Bitbucket API uses pagination
        params = {"pagelen": 100}  # Maximum allowed per page
        all_repos = []

        while True:
            response = self._make_request(endpoint, params=params)
            values = response.get("values", [])

            repos = []
            for repo in values:
                if repo.get("is_private") and not repo.get("has_access", True):
                    continue
                repos.append({
                    "name": repo.get("name"),
                    "full_name": repo.get("full_name"),
                    "url": repo.get("links", {}).get("self", {}).get("href"),
                    "language": repo.get("language"),
                    "description": repo.get("description"),
                    "updated_on": repo.get("updated_on")
                })

            all_repos.extend(repos)

            # Handle pagination
            next_page = response.get("next")
            if not next_page:
                break

            # For next page, we use the full URL
            params = None
            endpoint = next_page

        return all_repos

    @action(description="Gets details about a specific repository")
    def get_repository(self, workspace: Optional[str], repo_name: str) -> Dict[str, Any]:
        """Get repository details."""
        if "/" in repo_name:
            # If full path is provided (workspace/repo)
            workspace, repo_slug = repo_name.split("/")
        else:
            # Use provided workspace and repo name
            if not workspace:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        # Bitbucket uses repo_slug (URL-friendly version of the name)
        url = f"/repositories/{workspace}/{repo_slug}"

        response = self._make_request(url)
        return {
            "name": response.get("name"),
            "full_name": response.get("full_name"),
            "private": response.get("is_private"),
            "language": response.get("language"),
            "size": response.get("size"),
            "url": response.get("links", {}).get("self", {}).get("href"),
            "description": response.get("description"),
            "updated_on": response.get("updated_on"),
            "created_on": response.get("created_on")
        }

    @action(description="Gets recent commits made to the repository, default limit: 10")
    def get_recent_commits(self, workspace: Optional[str], repo_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent commits for a repository."""
        if "/" in repo_name:
            # If full path is provided (workspace/repo)
            workspace, repo_slug = repo_name.split("/")
        else:
            # Use provided workspace and repo name
            if not workspace:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        url = f"/repositories/{workspace}/{repo_slug}/commits"

        # Set pagination
        params = {"pagelen": min(limit, 100)}  # Limit to requested number or max allowed

        response = self._make_request(url, params=params)
        commits = []

        for commit in response.get("values", []):
            commits.append({
                "hash": commit.get("hash"),
                "author": commit.get("author", {}).get("user", {}).get("display_name"),
                "message": commit.get("message"),
                "parents": [parent["hash"] for parent in commit.get("parents", [])],
                "date": commit.get("date")
            })

            if len(commits) >= limit:
                break

        return commits

    def clone_repository(self, workspace: Optional[str], repo_name: str) -> str:
        """Clone a repository and return the local path."""
        if "/" in repo_name:
            # If full path is provided (workspace/repo)
            workspace, repo_slug = repo_name.split("/")
        else:
            # Use provided workspace and repo name
            if not workspace:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        # Construct the HTTPS clone URL with credentials
        url = f"https://x-token-auth:{self.config.access_token}@bitbucket.org/{workspace}/{repo_slug}.git"
        local_path = os.path.join(self.config.work_dir, repo_slug)

        if os.path.exists(local_path):
            # Pull latest changes if repo already exists
            os.chdir(local_path)
            # Set the remote URL with credentials before pulling
            subprocess.run(["git", "remote", "set-url", "origin", url], check=True)
            # Pull the latest changes
            subprocess.run(["git", "pull"], check=True)
        else:
            # Clone with credentials in URL
            subprocess.run(["git", "clone", url, local_path], check=True)
            os.chdir(local_path)
            # Remove credentials from recorded remote URL
            clean_url = f"https://bitbucket.org/{workspace}/{repo_slug}.git"
            subprocess.run(["git", "remote", "set-url", "origin", clean_url], check=True)
            # Configure credential helper
            subprocess.run(["git", "config", "--local", "credential.helper", "cache"], check=True)

        return local_path

    # @action(description="Searches for code in the workspace or repository")
    # def search_code(self, workspace: Optional[str], repo_name: Optional[str], query: str) -> List[Dict[str, Any]]:
    #     """
    #     Search for code in repositories using Bitbucket's code search API.
    #     """
    #     if not repo_name:
    #         self.logger.warning("Bitbucket requires a specific repository for code search")
    #         return []
    #
    #     # Extract workspace and repo_slug from repo_name if provided as "workspace/repo"
    #     if "/" in repo_name:
    #         workspace, repo_slug = repo_name.split("/", 1)  # Split on first occurrence only
    #     else:
    #         if not workspace:
    #             self.logger.error("Workspace is required when repo_name is not in 'workspace/repo' format")
    #             return []
    #         repo_slug = repo_name
    #
    #     url = f"/workspaces/{workspace}/search/code"
    #
    #     try:
    #         search_results = []
    #         next_page = None
    #
    #         while True:
    #             # Use pagination parameters if next_page is available
    #             params = {"search_query": query, "page": next_page} if next_page else {"search_query": query}
    #             response = self._make_request(url, params=params)
    #
    #             # Extract results from response
    #             for item in response.get("values", []):
    #                 file_info = item.get("file", {})
    #                 search_results.append({
    #                     "path": file_info.get("path"),
    #                     "type": file_info.get("type"),
    #                     "size": file_info.get("size"),
    #                     "links": file_info.get("links")
    #                 })
    #
    #             # Check for next page
    #             next_page = response.get("next")
    #             if not next_page:
    #                 break
    #
    #         return search_results
    #     except Exception as e:
    #         self.logger.error(f"Error searching code in Bitbucket: {e}")
    #         return []


# def main():
#     # Configure the agent
#     bitbucket_config = BitbucketConfig(
#         access_token=os.getenv("BITBUCKET_TOKEN")
#     )
#
#     # Create and run the agent
#     client = BitbucketClient(bitbucket_config)
#     print("=====================================================================")
    # print(f"List repos: {client.execute_action("list_workspaces", {})}")
    # print("=====================================================================")
    # print(f"List repos: {client.execute_action("list_repositories", {})}")
    # print("=====================================================================")
    # print(f"Get repos: {client.execute_action("get_repository", {"repo_name": "horus-ai-labs/DistillFlow"})}")
    # print("=====================================================================")
    # print(f"List files: {client.execute_action("list_files", {"repo_name": "horus-ai-labs/DistillFlow"})}")
    # print("=====================================================================")
    # print(f"Recent commits: {client.execute_action("get_recent_commits", {"repo_name": "horus-ai-labs/DistillFlow"})}")
    # print("=====================================================================")
    # print(f"Search code: {client.execute_action("search_code", {"workspace": "horus-ai-labs", "repo_name": "DistillFlow", "query": "load_tokenizer"})}")
    # print("=====================================================================")
    # print(f"Read file: {client.execute_action("read_file", {"repo_name": "horus-ai-labs/DistillFlow", "file_path": "README.rst"})}")
    # print("=====================================================================")
    # print(f"Git diff: {client.execute_action("get_commit_diff", {"repo_name": "horus-ai-labs/DistillFlow", "commit_hash": "368a10b5463ebf6feda0c329a7edc6ba73242ddc"})}")
    # print("=====================================================================")
    # print(f"Read all files: {client.execute_action("read_all_files", {"workspace": "horus-ai-labs", "repo_name": "DistillFlow"})}")


# if __name__ == "__main__":
#     main()