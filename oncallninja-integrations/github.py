import logging
import os
import subprocess

import requests
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from .action_router import action
from .code_client import CodingClient

class GitHubConfig(BaseModel):
    access_token: str = Field(..., description="GitHub Personal Access Token")
    api_url: str = Field("https://api.github.com", description="GitHub API URL")
    work_dir: str = Field("/tmp/oncallninja-repos", description="Working directory for cloning repos")

class GitHubClient(CodingClient):
    def __init__(self, config: GitHubConfig):
        super().__init__(config.work_dir)
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.headers = {
            "Authorization": f"Bearer {config.access_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        os.makedirs(self.config.work_dir, exist_ok=True)

    @action(description="Make a request to api.github.com/`endpoint` with the given `params` and `data`")
    def _make_request(self, endpoint: str, params: Dict = None, data: Dict = None) -> Any:
        """
        Make a request to the LaunchDarkly API.

        Args:
            endpoint: API endpoint to call (without base URL)
            params: Query parameters
            data: Body data for POST/PUT requests

        Returns:
            Response JSON as dictionary
        """
        url = f"{self.config.api_url}{endpoint}"

        try:
            response = requests.get(
                url=url,
                headers=self.headers,
                params=params,
                json=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error making request to Github API: {e}")
            if hasattr(e.response, 'text'):
                self.logger.error(f"Response content: {e.response.text}")
            raise

    @action(description="Lists all available repositories in the given `org_name`")
    def list_repositories(self, org_name: str) -> List[Dict[str, Any]]:
        """List all accessible repositories."""
        if org_name:
            endpoint = f"/orgs/{org_name}/repos"
        else:
            endpoint = "/user/repos"

        response = self._make_request(endpoint)
        repos = []
        for repo in response:
            if repo.get("archived") or repo.get("disabled"):
                continue
            repos.append({
                "name": repo.get("name"),
                "url": repo.get("url"),
                "language": repo.get("language"),
                "description": repo.get("description"),
                "pushed_at": repo.get("pushed_at")
            })

        return repos

    @action(description="Lists all available repositories in the given `org_name`")
    def list_repositories(self, org_name: str) -> List[Dict[str, Any]]:
        """List all accessible repositories."""
        if org_name:
            endpoint = f"/orgs/{org_name}/repos"
        else:
            endpoint = "/user/repos"

        response = self._make_request(endpoint)
        repos = []
        for repo in response:
            if repo.get("archived") or repo.get("disabled"):
                continue
            repos.append({
                "name": repo.get("name"),
                "url": repo.get("url"),
                "language": repo.get("language"),
                "description": repo.get("description"),
                "pushed_at": repo.get("pushed_at")
            })

        return repos

    @action(description="Gets details about a specific repository")
    def get_repository(self, org_name: Optional[str], repo_name: str) -> Dict[str, Any]:
        """Get repository details."""
        owner = repo_name.split("/")[0] if "/" in repo_name else org_name
        repo = repo_name.split("/")[-1]
        url = f"/repos/{owner}/{repo}"

        response = self._make_request(url)
        return {
            "name": response.get("full_name"),
            "private": response.get("private"),
            "archived": response.get("archived") or response.get("disabled"),
            "language": response.get("language"),
            "size": response.get("size"),
            "url": response.get("url"),
            "description": response.get("description"),
            "updated_at": response.get("updated_at"),
            "pushed_at": response.get("pushed_at")
        }

    @action(description="Gets recent commits made to the repository, default limit: 10")
    def get_recent_commits(self, org_name: Optional[str], repo_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent commits for a repository."""
        owner = repo_name.split("/")[0] if "/" in repo_name else org_name
        repo = repo_name.split("/")[-1]
        url = f"/repos/{owner}/{repo}/commits?per_page={limit}"
        response = self._make_request(url)
        commits = []
        for commit in response:
            commit_data = commit.get("commit")
            commits.append({
                "sha": commit.get("sha"),
                "author": commit_data.get("author").get("name"),
                "message": commit_data.get("message"),
                "parent": [parent["sha"] for parent in commit.get("parents", [])],
                "date": commit_data.get("committer").get("date")
            })

        return commits

    @action(description="Searches for code in github using github search; supports all github search keys")
    def search_code(self, org_name: str, repo_name: Optional[str], query: str) -> List[Dict[str, Any]]:
        """Search for code across repositories."""
        if "repo:" not in query and "user:" not in query:
            if org_name and repo_name:
                org_name = repo_name.split("/")[0] if "/" in repo_name else org_name
                repo = repo_name.split("/")[-1]
                query += f" AND repo:{org_name}/{repo}"
            elif org_name:
                query += f" AND user:{org_name}"

        url = f"/search/code?q={query}"

        response = self._make_request(url)
        items = response.get("items", [])
        search_result = []
        for item in items:
            search_result.append({
                "path": item.get("path"),
                "sha": item.get("sha"),
                "repository": item.get("repository").get("url"),
            })

        return search_result

    def clone_repository(self, org_name: Optional[str], repo_name: str) -> str:
        """Clone a repository and return the local path."""
        owner = repo_name.split("/")[0] if "/" in repo_name else org_name
        repo = repo_name.split("/")[-1]
        url = f"https://{self.config.access_token}@github.com/{owner}/{repo}.git"
        local_path = os.path.join(self.config.work_dir, repo)

        if os.path.exists(local_path):
            # Pull latest changes if repo already exists
            os.chdir(local_path)
            subprocess.run(["git", "remote", "set-url", "origin", url], check=True)
            subprocess.run(["git", "pull"], check=True)
        else:
            # Clone with token in URL but configure to not store credentials
            subprocess.run(["git", "clone", url, local_path], check=True)
            os.chdir(local_path)
            # Remove token from recorded remote URL
            subprocess.run(["git", "remote", "set-url", "origin", url], check=True)
            # Configure credential helper
            subprocess.run(["git", "config", "--local", "credential.helper", "cache"], check=True)

        return local_path

# Command Line Interface
def main():
    # Configure the agent
    github_config = GitHubConfig(
        access_token=os.getenv("GITHUB_TOKEN")
    )

    # Create and run the agent
    client = GitHubClient(github_config)
    print("=====================================================================")
    # print(f'List repos: {client.execute_action("list_repositories", {"org_name": "horus-ai-labs"})}')
    # print("=====================================================================")
    # print(f'Get repos: {client.execute_action("get_repository", {"repo_name": "horus-ai-labs/DistillFlow"})}')
    # print("=====================================================================")
    # print(f'List files: {client.execute_action("list_files", {"repo_name": "horus-ai-labs/DistillFlow"})}')
    # print("=====================================================================")
    # print(f'Recent commits: {client.execute_action("get_recent_commits", {"repo_name": "horus-ai-labs/DistillFlow"})}')
    print("=====================================================================")
    print(f'Search code: {client.execute_action("search_code", {"org_name": "horus-ai-labs", "query": "load_tokenizer"})}')
    print("=====================================================================")
    # print(f'ls: {client.execute_action("list_files", {"repo_name": "horus-ai-labs/DistillFlow"})}')
    # print("=====================================================================")
    # print(f'Read file: {client.execute_action("read_file", {"repo_name": "horus-ai-labs/DistillFlow", "file_path": "README.rst"})}')
    # print("=====================================================================")
    # print(f'Read file: {client.execute_action("get_commit_diff", {"repo_name": "horus-ai-labs/DistillFlow", "commit_hash": "368a10b5463ebf6feda0c329a7edc6ba73242ddc"})}')

if __name__ == "__main__":
    main()