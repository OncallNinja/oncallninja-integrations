import fnmatch
import logging
import os
import subprocess
from pathlib import Path

from typing import List, Dict, Any, Optional

from .action_router import action, ActionRouter

class CodingClient(ActionRouter):
    def __init__(self, work_dir: str):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.work_dir = Path(work_dir)
        os.makedirs(work_dir, exist_ok=True)

    @action(description="clone the repository locally")
    def clone_repository(self, workspace: Optional[str], repo_name: str) -> str:
        raise NotImplementedError("Coding clients must implement the convert method.")

    @action(description="Lists files in a github repository")
    def list_files(self, org_name: Optional[str], repo_name: str, path: str = "") -> List[str]:
        """List files in a repository directory."""
        repo_path = self.clone_repository(org_name, repo_name)

        full_path = os.path.join(repo_path, path)
        result = []

        for root, dirs, files in os.walk(full_path):
            rel_path = os.path.relpath(root, repo_path)
            if rel_path == ".":
                rel_path = ""

            for file in files:
                if not file.startswith(".git"):
                    file_path = os.path.join(rel_path, file)
                    result.append(file_path)

        return result

    @action(description="Reads a file's contents")
    def read_file(self, org_name: Optional[str], repo_name: str, file_path: str) -> str:
        """Read content of a file in the repository."""

        repo_path = self.clone_repository(org_name, repo_name)

        full_path = os.path.join(repo_path, file_path)
        with open(full_path, "r", encoding="utf-8") as f:
            return f.read()

    @action(
        description="Read all files contents, returns `path`, `content`, `size` and `is_directory` per file in the given `path`")
    def read_all_files(self, workspace: Optional[str], repo_name: str, path: Optional[str]) -> List[Dict[str, Any]]:
        """Read content of a file in the repository."""

        all_files = []
        repo_path = self.clone_repository(workspace, repo_name)

        # If path is provided, adjust the target directory
        target_path = repo_path if path is None else os.path.join(repo_path, path)

        # Check if the path exists
        if not os.path.exists(target_path):
            raise FileNotFoundError(f"Path not found: {path}")

        # Walk through the repository files
        for root, dirs, files in os.walk(target_path):
            dirs[:] = [d for d in dirs if not d.startswith('.')]

            for file_name in files:
                # Skip binary/common non-code files (adjust patterns as needed)
                file_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(file_path, repo_path)

                try:
                    # Try to read the file as text
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    all_files.append({
                        'path': relative_path,
                        'content': content,
                        'size': os.path.getsize(file_path),
                    })
                except UnicodeDecodeError:
                    # Skip binary files
                    continue
                except Exception as e:
                    # Log errors but continue processing other files
                    print(f"Error reading file {file_path}: {str(e)}")

        return all_files

    @action(description="Gets details about the commit from the local repository")
    def get_commit_details(self, org_name: Optional[str], repo_name: str, limit: int = 10) -> List[Dict[str, str]]:
        """Get recent commit details from a local repository."""
        repo_path = self.clone_repository(org_name, repo_name)
        os.chdir(repo_path)
        result = subprocess.run(
            ["git", "log", f"-{limit}", "--pretty=format:%H|%an|%ad|%s"],
            capture_output=True,
            text=True,
            check=True
        )

        commits = []
        for line in result.stdout.strip().split("\n"):
            if line:
                parts = line.split("|")
                if len(parts) >= 4:
                    commits.append({
                        "hash": parts[0],
                        "author": parts[1],
                        "date": parts[2],
                        "message": parts[3]
                    })

        return commits

    @action(description="Gets diff for a commit in git diff format")
    def get_commit_diff(self, org_name: Optional[str], repo_name: str, commit_hash: str) -> str:
        """Get diff for a specific commit."""
        repo_path = self.clone_repository(org_name, repo_name)
        os.chdir(repo_path)
        result = subprocess.run(
            ["git", "show", commit_hash],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout

    @action(description="Searches for code in the given workspace and repository from the local")
    def search_code(self, workspace: str, repo_name: str, query: str) -> List[Dict[str, Any]]:
        """
        Search for code in a locally cloned repository
        """
        search_results = []

        try:
            # Clone the repository locally
            repo_path = self.clone_repository(workspace, repo_name)

            # Walk through the repository files
            for root, dirs, files in os.walk(repo_path):
                for file_name in files:
                    # Skip binary/common non-code files (adjust patterns as needed)
                    if fnmatch.fnmatch(file_name, '*.py') or \
                            fnmatch.fnmatch(file_name, '*.js') or \
                            fnmatch.fnmatch(file_name, '*.java') or \
                            fnmatch.fnmatch(file_name, '*.md') or \
                            fnmatch.fnmatch(file_name, '*.txt'):

                        file_path = os.path.join(root, file_name)
                        relative_path = os.path.relpath(file_path, repo_path)

                        try:
                            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                line_number = 0
                                matches = []

                                for line in f:
                                    line_number += 1
                                    if query in line:
                                        matches.append({
                                            'line': line_number,
                                            'snippet': line.strip()[:200]  # Truncate long lines
                                        })

                                if matches:
                                    search_results.append({
                                        'path': relative_path,
                                        'matches': matches,
                                        'size': os.path.getsize(file_path)
                                    })
                        except UnicodeDecodeError:
                            self.logger.warning(f"Skipped binary file: {relative_path}")

            return search_results

        except Exception as e:
            self.logger.error(f"Local search failed: {str(e)}")
            return []