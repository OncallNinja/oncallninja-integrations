import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone # Added for timestamp comparison

import requests
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from .action_router import action
from .code_client import CodingClient

log = logging.getLogger(__name__)

class BitbucketConfig(BaseModel):
    access_tokens: Dict[str, str] = Field(..., description="Map of repository names to their Bitbucket access tokens")
    api_url: str = Field("https://api.bitbucket.org/2.0", description="Bitbucket API URL")
    work_dir: str = Field("/tmp/oncallninja-repos", description="Working directory for cloning repos")
    max_commits_to_analyze: int = Field(10, description="Maximum number of recent commits to analyze")
    issue_timestamp: Optional[str] = Field(None, description="Timestamp of the related issue for context") # Added

class BitbucketClient(CodingClient):
    def __init__(self, config: BitbucketConfig):
        super().__init__(config.work_dir)
        self.config = config
        self.issue_timestamp = config.issue_timestamp # Store the timestamp
        # Bitbucket uses Basic Auth with username and app password
        self.token_map = config.access_tokens
        # Initialize with empty headers, will be set per request
        self.headers = {"Accept": "application/json", "Content-Type": "application/json"}
        os.makedirs(self.config.work_dir, exist_ok=True)

    def _get_token_for_org(self, repo_name: str) -> str:
        """Get the access token for a specific repository."""
        if repo_name not in self.token_map:
            raise ValueError(f"No access token found for repository: {repo_name}")
        return self.token_map[repo_name]

    def _get_headers_for_org(self, repo_name: str) -> Dict[str, str]:
        """Get headers with appropriate token for a repository."""
        token = self._get_token_for_org(repo_name)
        return {
            **self.headers,
            "Authorization": f"Bearer {token}"
        }

    def _make_request(self, org_name: str, endpoint: str, params: Dict = None, method: str = "GET", data: Dict = None) -> Any:
        """
        Make a request to the Bitbucket API.

        Args:
            endpoint: API endpoint to call (without base URL)
            params: Query parameters
            method: HTTP method (GET, POST, PUT, DELETE)
            data: Request body for POST, PUT requests
            org_name: Repository name to determine which token to use

        Returns:
            Response JSON as dictionary
        """

        url = endpoint if self.config.api_url in endpoint else f"{self.config.api_url}{endpoint}"

        # Use repo-specific headers if repo_name is provided
        headers = self._get_headers_for_org(org_name) if org_name else self.headers

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
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")


            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Bitbucket API: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise

    @action(description="Lists all accessible org_names")
    def list_all_orgs(self) -> List[str]:
        """List all accessible repositories."""

        # Bitbucket API uses pagination
        params = {"pagelen": 100}  # Maximum allowed per page
        all_org_names = set()

        endpoint = "/repositories?role=member"
        for org_name in self.token_map:
            while True:
                response = self._make_request(org_name, endpoint, params=params)
                values = response.get("values", [])

                for org in values:
                    if org.get("is_private") and not org.get("has_access", True):
                        continue
                    all_org_names.add(org.get("full_name").split("/")[0])

                # Handle pagination
                next_page = response.get("next")
                if not next_page:
                    break

                # For next page, we use the full URL
                params = None
                endpoint = next_page

        return list(all_org_names)

    @action(description="Lists all accessible repositories, filter repositories by passing specific org_name")
    def list_repositories(self, filter_org_name: Optional[str]) -> List[Dict[str, Any]]:
        """List all accessible repositories."""
        if not filter_org_name:
            # Get user's repositories
            endpoint = "/repositories?role=member"
        else:
            endpoint = f"/repositories/{filter_org_name}"

        # Bitbucket API uses pagination
        params = {"pagelen": 100}  # Maximum allowed per page
        all_repos = []

        for org in self.token_map:
            if filter_org_name and org != filter_org_name:
                continue
            while True:
                response = self._make_request(org, endpoint, params=params)
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
    def get_repository(self, org_name: Optional[str], repo_name: str) -> Dict[str, Any]:
        """Get repository details."""
        if "/" in repo_name:
            # If full path is provided (org_name/repo)
            org_name, repo_slug = repo_name.split("/")
        else:
            # Use provided org_name and repo name
            if not org_name:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        # Bitbucket uses repo_slug (URL-friendly version of the name)
        url = f"/repositories/{org_name}/{repo_slug}"

        response = self._make_request(org_name, url)
        return {
            "name": response.get("name"),
            "full_name": response.get("full_name"),
            "private": response.get("is_private"),
            "language": response.get("language"),
            "size": response.get("size"),
            "url": response.get("links", {}).get("self", {}).get("href"),
            "description": response.get("description"),
            "updated_on": response.get("updated_on"),
            "created_on": response.get("created_on"),
            "main_branch": response.get("mainbranch", {}).get("name") # Added main_branch
        }

    @action(description="Gets recent commits made to the repository, always limited to the main branch, default limit: 10")
    def get_recent_commits(self, org_name: Optional[str], repo_name: str, limit: Optional[int] = 10) -> List[Dict[str, Any]]:
        """Get recent commits for a repository's main branch."""
        if "/" in repo_name:
            org_name, repo_slug = repo_name.split("/")
        else:
            if not org_name:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        if not limit:
            limit = 10
        if isinstance(limit, str):
            limit = int(limit)

        # Get repository details to find the main branch
        repo_details = self.get_repository(org_name, repo_slug)
        main_branch = repo_details.get("main_branch")

        if not main_branch:
            log.warning(f"Could not determine main branch for {org_name}/{repo_slug}. Falling back to 'master' or 'main'.")
            # Attempt common default branch names if not found
            # This is a fallback and might not always be accurate
            try:
                # Try to fetch commits for 'main' branch
                self._make_request(org_name, f"/repositories/{org_name}/{repo_slug}/commits/main", params={"pagelen": 1})
                main_branch = "main"
            except requests.exceptions.RequestException:
                try:
                    # If 'main' fails, try 'master'
                    self._make_request(org_name, f"/repositories/{org_name}/{repo_slug}/commits/master", params={"pagelen": 1})
                    main_branch = "master"
                except requests.exceptions.RequestException:
                    raise ValueError(f"Could not determine a valid main branch for {org_name}/{repo_slug}. Please specify a branch if this is not 'main' or 'master'.")


        url = f"/repositories/{org_name}/{repo_slug}/commits/{main_branch}"

        params = {"pagelen": min(limit, 100)}

        response = self._make_request(org_name, url, params=params)
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

    def get_commit_before_timestamp(self, org_name: str, repo_slug: str, timestamp_str: str) -> Optional[str]:
        """
        Find the hash of the latest commit made strictly before the given timestamp.

        Args:
            org_name: The org_name containing the repository.
            repo_slug: The slug of the repository.
            timestamp_str: The timestamp string (format 'YYYY-MM-DD HH:MM:SS') to compare against.

        Returns:
            The commit hash as a string, or None if no commit is found before the timestamp.
        """
        print(f"Searching for commit before '{timestamp_str}' in {org_name}/{repo_slug}")

        try:
            # Parse the input timestamp string (assuming naive local time) and make it UTC
            # Bitbucket API returns dates in ISO 8601 format with timezone (usually UTC)
            target_dt_naive = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            # Assuming the input timestamp is local, convert to UTC for comparison
            # If the input is already UTC, this might need adjustment or clarification
            target_dt_utc = target_dt_naive.replace(tzinfo=timezone.utc) # Simplistic assumption: treat input as UTC
            log.debug(f"Target timestamp parsed to UTC: {target_dt_utc.isoformat()}")

        except ValueError as e:
            print(f"Error parsing input timestamp '{timestamp_str}': {e}")
            return None

        endpoint = f"/repositories/{org_name}/{repo_slug}/commits"
        params = {"pagelen": 50, "sort": "-date"} # Request 50 commits per page, sorted newest first

        while endpoint:
            try:
                log.debug(f"Fetching commits from endpoint: {endpoint}")
                response = self._make_request(org_name, endpoint, params=params)
                commits_data = response.get("values", [])
                if not commits_data:
                    print("No more commits found.")
                    break # No commits on this page

                for commit in commits_data:
                    commit_hash = commit.get("hash")
                    commit_date_str = commit.get("date")
                    if not commit_date_str or not commit_hash:
                        log.warning(f"Skipping commit with missing date or hash: {commit}")
                        continue

                    try:
                        # Parse commit date (ISO 8601 format)
                        commit_dt = datetime.fromisoformat(commit_date_str.replace("Z", "+00:00"))
                        log.debug(f"Comparing commit {commit_hash} ({commit_dt.isoformat()}) with target {target_dt_utc.isoformat()}")

                        # Compare timezone-aware datetimes
                        if commit_dt < target_dt_utc:
                            print(f"Found commit {commit_hash} before target timestamp.")
                            return commit_hash
                    except ValueError as e:
                        print(f"Error parsing commit date '{commit_date_str}' for commit {commit_hash}: {e}")
                        continue # Skip commit if date parsing fails

                # Prepare for next page
                endpoint = response.get("next")
                params = None # 'next' URL includes parameters
                log.debug(f"Moving to next page: {endpoint}")

            except requests.exceptions.RequestException as e:
                print(f"API error fetching commits: {e}")
                return None # Stop searching on API error
            except Exception as e:
                print(f"Unexpected error processing commits: {e}")
                return None # Stop on unexpected errors

        print(f"No commit found strictly before {timestamp_str} in {org_name}/{repo_slug}")
        return None

    @action(description="clone the repository locally")
    def clone_repository(self, org_name: Optional[str], repo_name: str) -> str:
        """Clone a repository and return the local path."""
        if "/" in repo_name:
            # If full path is provided (org_name/repo)
            org_name, repo_slug = repo_name.split("/")
        else:
            # Use provided org_name and repo name
            if not org_name:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        # Construct the HTTPS clone URL with credentials
        token = self.token_map[org_name]
        url = f"https://x-token-auth:{token}@bitbucket.org/{org_name}/{repo_slug}.git"
        local_path = os.path.join(self.config.work_dir, repo_slug)

        if os.path.exists(local_path):
            # Pull latest changes if repo already exists
            os.chdir(local_path)

            try:
                # Set the remote URL with credentials before pulling
                subprocess.run(["git", "remote", "set-url", "origin", url], check=True)

                # Check if we're in detached HEAD state or have local changes
                head_state = subprocess.run(["git", "symbolic-ref", "--quiet", "HEAD"],
                                            capture_output=True, text=True)

                # If in detached HEAD state or any other issues, reset to main branch
                if head_state.returncode != 0:
                    print("Repository is in detached HEAD state, attempting to fix...")

                    # Fetch all branches
                    subprocess.run(["git", "fetch", "origin"], check=True)

                    # Get default branch from remote
                    default_branch = subprocess.run(
                        ["git", "remote", "show", "origin"],
                        capture_output=True, text=True, check=True
                    )

                    # Parse the output to find the default branch (usually HEAD branch)
                    main_branch = "master"
                    for line in default_branch.stdout.splitlines():
                        if "HEAD branch:" in line:
                            main_branch = line.split(":")[-1].strip()
                            print(f"Default branch is: {main_branch}")
                            break

                    print(f"Resetting to origin/{main_branch}")

                    # Try to checkout the main branch from remote
                    subprocess.run(["git", "checkout", "-B", main_branch, f"origin/{main_branch}"], check=True)

                # Check if there are local changes
                result = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True)

                if result.stdout.strip():
                    # Log the changes
                    changes = subprocess.run(["git", "status"], check=True, capture_output=True, text=True)
                    print(f"Local changes detected: {changes.stdout}, removing...")

                    subprocess.run(["git", "reset", "--hard", "HEAD"], check=True)

                try:
                    # Verify current branch again before pulling
                    current_branch = subprocess.run(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        capture_output=True, text=True, check=True
                    ).stdout.strip()

                    if current_branch == "HEAD":
                        raise subprocess.CalledProcessError(1, "git checkout", "Failed to get onto a branch")

                    # Pull the latest changes
                    print(f"Pulling latest changes into {current_branch}...")
                    subprocess.run(["git", "pull", "origin", current_branch], check=True)
                    print("Successfully pulled latest changes")

                except subprocess.CalledProcessError as e:
                    print(f"Error pulling changes: {e}")
                    # If pull still fails, try more aggressive approach

                    print("Attempting more aggressive reset...")
                    # Fetch all
                    subprocess.run(["git", "fetch", "--all"], check=True)
                    # Hard reset to remote
                    current_branch = subprocess.run(
                        ["git", "branch", "--show-current"],
                        capture_output=True, text=True, check=True
                    ).stdout.strip()

                    if not current_branch:
                        # If still no current branch, determine default branch
                        for line in subprocess.run(
                                ["git", "remote", "show", "origin"],
                                capture_output=True, text=True, check=True
                        ).stdout.splitlines():
                            if "HEAD branch:" in line:
                                current_branch = line.split(":")[-1].strip()
                                break
                        else:
                            current_branch = "master"  # Default to master

                    # Reset to remote branch
                    subprocess.run(["git", "reset", "--hard", f"origin/{current_branch}"], check=True)
                    print(f"Reset to origin/{current_branch}")
            except subprocess.CalledProcessError as reset_error:
                print(f"Fatal error, could not reset: {reset_error}")
                # Last resort: delete and re-clone
                print("Deleting and re-cloning repository...")
                os.chdir(self.config.work_dir)
                shutil.rmtree(local_path, ignore_errors=True)
                subprocess.run(["git", "clone", url, local_path], check=True)
        else:
            # Clone with credentials in URL
            subprocess.run(["git", "clone", url, local_path], check=True)
            os.chdir(local_path)
            # Remove credentials from recorded remote URL
            clean_url = f"https://bitbucket.org/{org_name}/{repo_slug}.git"
            subprocess.run(["git", "remote", "set-url", "origin", clean_url], check=True)
            # Configure credential helper
            subprocess.run(["git", "config", "--local", "credential.helper", "cache"], check=True)

        # --- Checkout specific commit based on timestamp ---
        if self.issue_timestamp:
            print(f"Issue timestamp provided ({self.issue_timestamp}), attempting to find commit before this time.")
            commit_hash_to_checkout = self.get_commit_before_timestamp(org_name, repo_slug, self.issue_timestamp)

            if commit_hash_to_checkout:
                print(f"Checking out commit: {commit_hash_to_checkout}")
                try:
                    # Ensure we are in the correct directory before checkout
                    os.chdir(local_path)
                    subprocess.run(["git", "checkout", commit_hash_to_checkout], check=True, capture_output=True)
                    print(f"Successfully checked out commit {commit_hash_to_checkout}")
                except subprocess.CalledProcessError as e:
                    print(f"Failed to checkout commit {commit_hash_to_checkout}: {e}")
                    print(f"Git stderr: {e.stderr.decode()}")
                    # Decide if we should raise an error or just log and return the path
                    # For now, log the error and continue, returning the path to the repo head
                except Exception as e:
                    print(f"An unexpected error occurred during checkout: {e}")
            else:
                log.warning(
                    f"Could not find a commit before {self.issue_timestamp}. Repository remains at the latest commit.")
        else:
            print("No issue timestamp provided, repository remains at the latest commit.")
        # --- End Checkout ---

        return local_path


    @action(description="Creates a pull request in the specified repository")
    def create_pull_request(self, org_name: Optional[str], repo_name: str, new_branch_name: str,
                           base_branch: str, title: str, description: str = "", close_source_branch: bool = False) -> Dict[str, Any]:
        """
        Create a pull request in the specified repository.

        Args:
            org_name: The org_name where the repository is located
            repo_name: The name of the repository (can be in format "org_name/repo")
            new_branch_name: The source branch name
            base_branch: The destination branch name
            title: The title of the pull request
            description: The description of the pull request
            close_source_branch: Whether to close the source branch after merge

        Returns:
            Dictionary containing the created pull request details
        """
        if "/" in repo_name:
            # If full path is provided (org_name/repo)
            org_name, repo_slug = repo_name.split("/")
        else:
            # Use provided org_name and repo name
            if not org_name:
                raise ValueError("Workspace must be provided if repo_name doesn't include it")
            repo_slug = repo_name

        url = f"/repositories/{org_name}/{repo_slug}/pullrequests"

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

        full_repo_name = f"{org_name}/{repo_slug}"

        # Make POST request to create the PR using repo-specific headers
        try:
            result = self._make_request(org_name, url, data=data, method="POST")

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
            print(f"Error creating pull request: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise

    @action(description="Commits all local changes and pushes to a new Bitbucket branch")
    def commit_changes(self, org_name: Optional[str], repo_name: str,
                        commit_message: str, new_branch_name: str, base_branch: str = "main") -> None:
        """
        Commits all local changes, creates a new branch, and pushes to that Bitbucket branch.

        Args:
            repo_name: The name of the repository (can be in format "org_name/repo")
            new_branch_name: The name of the new branch to create
            base_branch: The branch to branch off from (default: main)
        """
        if "/" in repo_name:
            # If full path is provided (org_name/repo)
            org_name, repo_slug = repo_name.split("/")
        else:
            # Use provided org_name and repo name
            if not org_name:
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
            token = self.token_map[org_name]
            push_url = f"https://x-token-auth:{token}@bitbucket.org/{org_name}/{repo_slug}.git"
            subprocess.run(["git", "push", push_url, new_branch_name], check=True)
            print(f"Successfully created branch '{new_branch_name}' pushed to Bitbucket.")

        except subprocess.CalledProcessError as e:
            print(f"Error during commit and push: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    # @action(description="Searches for code in the org_name or repository")
    # def search_code_across_org(self, org_name: Optional[str], query: str) -> List[Dict[str, Any]]:
    #     """
    #     Search for code in repositories using Bitbucket's code search API.
    #     """
    #     if not org_name:
    #         log.warning("Bitbucket requires a specific repository for code search")
    #         return []
    #
    #     url = f"/org_names/{org_name}/search/code"
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
    #         print(f"Error searching code in Bitbucket: {e}")
    #         return []

# Command Line Interface
# def main():
#     # Configure the agent
#     github_config = BitbucketConfig(
#         access_tokens={"nanonets": os.getenv("BITBUCKET_TOKEN")}
#     )

    # # Create and run the agent
    # client = BitbucketClient(github_config)
    # print("=====================================================================")
    # print(f'List repos: {client.execute_action("list_repositories", {"org_name": "horus-ai-labs"})}')
    # # print("=====================================================================")
    # print(f'Get repos: {client.execute_action("get_repository", {"org_name": "horus-ai-labs", "repo_name": "DistillFlow"})}')
    # # print("=====================================================================")
    # print(f'List files: {client.execute_action("list_files", {"org_name": "horus-ai-labs", "repo_name": "DistillFlow"})}')
    # print("=====================================================================")
    # print(f'Recent commits: {client.execute_action("get_recent_commits", {"repo_name": "nanonets/nanonets_react_app"})}')
    # print("=====================================================================")
    # print(f'Search code: {client.execute_action("search_code_across_org", {"org_name": "horus-ai-labs", "query": "load_tokenizer"})}')
    # print("=====================================================================")
    # print(f'ls: {client.execute_action("list_files", {"repo_name": "horus-ai-labs/DistillFlow"})}')
    # print("=====================================================================")
    # print(f'Read file: {client.execute_action("read_file", {"repo_name": "horus-ai-labs/DistillFlow", "file_path": "README.rst"})}')
    # print("=====================================================================")
#     print(f'Read file: {client.execute_action("get_commit_diff", {"repo_name": "nanonets/nanonets_react_app", "commit_hash": "76a6d2b2820e0c0dffee8132cdff4d8e21a5b2f2"})}')
#
# if __name__ == "__main__":
#     main()
