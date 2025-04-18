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

log = logging.getLogger(__name__)

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
    def _make_request(self, endpoint: str, params: Dict = None, method: str = "GET", data: Dict = None) -> Any:
        """
        Make a request to the Bitbucket API.

        Args:
            endpoint: API endpoint to call (without base URL)
            params: Query parameters
            method: HTTP method (GET, POST, PUT, DELETE)
            data: Request body for POST, PUT requests

        Returns:
            Response JSON as dictionary
        """

        url = endpoint if self.config.api_url in endpoint else f"{self.config.api_url}{endpoint}"

        headers = self.headers.copy()  # Create a copy to avoid modifying the original
        if method in ("POST", "PUT"):
            headers["Content-Type"] = "application/json"

        try:
            if method == "GET":
                response = requests.get(
                    url=url,
                    headers=headers,
                    params=params
                )
            elif method == "POST":
                response = requests.post(
                    url=url,
                    headers=headers,
                    json=data
                )
            elif method == "PUT":
                response = requests.put(
                    url=url,
                    headers=headers,
                    json=data
                )
            elif method == "DELETE":
                response = requests.delete(
                    url=url,
                    headers=headers,
                    params=params # or json=data if needed for DELETE
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")


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

            # Check if there are local changes
            result = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True)

            if result.stdout.strip():
                # Log the changes
                changes = subprocess.run(["git", "status"], check=True, capture_output=True, text=True)
                log.info(f"Local changes detected: {changes.stdout}, removing...")

                subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)
            try:
                # Pull the latest changes
                subprocess.run(["git", "pull"], check=True)
                log.info("Successfully pulled latest changes")

            except subprocess.CalledProcessError as e:
                log.info(f"Error pulling changes: {e}")
                # Handle the error (retry, notify user, etc.)
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


    @action(description="Creates a pull request in the specified repository")
    def create_pull_request(self, workspace: Optional[str], repo_name: str, new_branch_name: str,
                           base_branch: str, title: str, description: str = "", close_source_branch: bool = False) -> Dict[str, Any]:
        """
        Create a pull request in the specified repository.

        Args:
            workspace: The workspace where the repository is located
            repo_name: The name of the repository (can be in format "workspace/repo")
            new_branch_name: The source branch name
            base_branch: The destination branch name
            title: The title of the pull request
            description: The description of the pull request
            close_source_branch: Whether to close the source branch after merge

        Returns:
            Dictionary containing the created pull request details
        """
        if "/" in repo_name:
            # If full path is provided (workspace/repo)
            workspace, repo_slug = repo_name.split("/")
        else:
            # Use provided workspace and repo name
            if not workspace:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        url = f"/repositories/{workspace}/{repo_slug}/pullrequests"

        # Prepare the request data
        data = {
            "title": title,
            "description": description,
            "source": {
                "branch": {
                    "name": new_branch_name
                }
            },
            "destination": {
                "branch": {
                    "name": base_branch
                }
            },
            "close_source_branch": close_source_branch
        }

        full_repo_name = f"{workspace}/{repo_slug}"

        # Make POST request to create the PR using repo-specific headers
        try:
            result = self._make_request(url, data=data, method="POST")

            return {
                "id": result.get("id"),
                "title": result.get("title"),
                "description": result.get("description"),
                "state": result.get("state"),
                "new_branch_name": result.get("source", {}).get("branch", {}).get("name"),
                "base_branch": result.get("destination", {}).get("branch", {}).get("name"),
                "author": result.get("author", {}).get("display_name"),
                "created_on": result.get("created_on"),
                "updated_on": result.get("updated_on"),
                "url": result.get("links", {}).get("html", {}).get("href")
            }
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error creating pull request: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.logger.error(f"Response content: {e.response.text}")
            raise

    @action(description="Commits all local changes and pushes to a new Bitbucket branch")
    def commit_changes(self, workspace: Optional[str], repo_name: str,
                        commit_message: str, new_branch_name: str, base_branch: str = "main") -> None:
        """
        Commits all local changes, creates a new branch, and pushes to that Bitbucket branch.

        Args:
            repo_name: The name of the repository (can be in format "workspace/repo")
            new_branch_name: The name of the new branch to create
            base_branch: The branch to branch off from (default: main)
        """
        if "/" in repo_name:
            # If full path is provided (workspace/repo)
            workspace, repo_slug = repo_name.split("/")
        else:
            # Use provided workspace and repo name
            if not workspace:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        try:
            # 1. Create new branch
            subprocess.run(["git", "checkout", "-b", new_branch_name, base_branch], check=True)

            # 2. Add all changes
            subprocess.run(["git", "add", "."], check=True)

            # 3. Commit changes
            subprocess.run(["git", "commit", "-m", commit_message], check=True)

            # 4. Push to the new branch
            push_url = f"https://x-token-auth:{self.config.access_token}@bitbucket.org/{workspace}/{repo_slug}.git"
            subprocess.run(["git", "push", push_url, new_branch_name], check=True)
            self.logger.info(f"Successfully created branch '{new_branch_name}' pushed to Bitbucket.")

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error during commit and push: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            raise



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
