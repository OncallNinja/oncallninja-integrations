import os
from datetime import datetime, timedelta

import requests
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from ldclient import Config, LDClient, Context

from .action_router import action, ActionRouter


@dataclass
class FlagIssue:
    """
    Represents a potential issue with a feature flag configuration.
    """
    flag_key: str
    project_key: str
    environment_key: str
    user_key: Optional[str]
    expected_value: Optional[Any]
    actual_value: Any
    issue_type: str  # e.g., "wrong_value", "unexpectedly_on", "unexpectedly_off"
    description: str

class LaunchDarklyClient(ActionRouter):
    """
    Integration with LaunchDarkly for checking feature flag statuses and configurations.
    Uses the LaunchDarkly REST API to fetch information without SDK key dependencies.
    """

    BASE_URL = "https://app.launchdarkly.com/api/v2"

    def __init__(self, api_key: str, sdk_key: str):
        """
        Initialize the LaunchDarkly integration with your API key.

        Args:
            api_key: LaunchDarkly REST API key with appropriate permissions
        """
        super().__init__()
        self.api_key = api_key
        self.sdk_key = sdk_key
        self.headers = {
            "Authorization": f"{api_key}",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)

        # Initialize the SDK client if SDK key is provided
        self.ld_client = None
        if sdk_key:
            config = Config(sdk_key)
            self.ld_client = LDClient(config)
            if not self.ld_client.is_initialized():
                self.logger.error("LaunchDarkly SDK client failed to initialize")
            self.logger.info("LaunchDarkly SDK client initialized successfully")

    def __del__(self):
        """Clean up SDK client resources when the object is destroyed."""
        if self.ld_client:
            self.ld_client.close()

    @action(description="Make a request to app.launchdarkly.com/api/v2/`endpoint` with the given `params` and `data`")
    def _make_request(self, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """
        Make a request to the LaunchDarkly API.

        Args:
            endpoint: API endpoint to call (without base URL)
            params: Query parameters
            data: Body data for POST/PUT requests

        Returns:
            Response JSON as dictionary
        """
        url = f"{self.BASE_URL}{endpoint}"

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
            self.logger.error(f"Error making request to LaunchDarkly API: {e}")
            if hasattr(e.response, 'text'):
                self.logger.error(f"Response content: {e.response.text}")
            raise

    @action(description="Lists all projects in LaunchDarkly")
    def list_projects(self) -> List[Dict]:
        """
        List all projects in the LaunchDarkly account with simplified information.

        Returns:
            List of simplified project dictionaries with key, name, and links
        """
        response = self._make_request("/projects")

        simplified_projects = []
        for project in response.get("items", []):
            links = {}
            if "_links" in project:
                for link_name, link_data in project["_links"].items():
                    links[link_name] = link_data.get("href")

            simplified_projects.append({
                "key": project.get("key"),
                "name": project.get("name"),
                "tags": project.get("tags", []),
                "links": links
            })

        return simplified_projects

    @action(description="Lists all environments in the given `project_key`")
    def list_environments(self, project_key: str) -> List[Dict]:
        """
        List all environments for a specific project with simplified information.

        Args:
            project_key: Project key identifier

        Returns:
            List of simplified environment dictionaries
        """
        response = self._make_request(f"/projects/{project_key}/environments")

        simplified_environments = []
        for env in response.get("items", []):
            links = {}
            if "_links" in env:
                for link_name, link_data in env["_links"].items():
                    links[link_name] = link_data.get("href")

            simplified_environments.append({
                "key": env.get("key"),
                "name": env.get("name"),
                "is_production": env.get("production", False),
                "is_default": env.get("default", False),
            })

        return simplified_environments

    @action(description="Lists all feature flags in the given `project_key`")
    def list_feature_flags(self, project_key: str) -> List[Dict]:
        """
        List all feature flags for a specific project with simplified information.

        Args:
            project_key: Project key identifier

        Returns:
            List of simplified feature flag dictionaries
        """
        response = self._make_request(f"/flags/{project_key}")

        simplified_flags = []
        for flag in response.get("items", []):
            links = {}
            if "_links" in flag:
                for link_name, link_data in flag["_links"].items():
                    links[link_name] = link_data.get("href")

            # Extract environment statuses if available
            environments = {}
            if "environments" in flag:
                for env_key, env_data in flag["environments"].items():
                    environments[env_key] = {
                        "on": env_data.get("on", False),
                        "last_modified": env_data.get("lastModified"),
                        "version": env_data.get("version")
                    }

            simplified_flags.append({
                "key": flag.get("key"),
                "name": flag.get("name"),
                "description": flag.get("description"),
                "tags": flag.get("tags", []),
                "variation_type": flag.get("variationType"),
                "temporary": flag.get("temporary", False),
                "client_side_availability": flag.get("clientSideAvailability"),
                "created_date": flag.get("creationDate"),
                "environments": environments,
                "links": links
            })

        return simplified_flags

    @action(description="Gets details about feature flag `flag_key` in the given `project_key`")
    def get_feature_flag(self, project_key: str, flag_key: str) -> Dict:
        """
        Get detailed information about a specific feature flag with simplified format.

        Args:
            project_key: Project key identifier
            flag_key: Feature flag key identifier

        Returns:
            Simplified feature flag details dictionary
        """
        flag = self._make_request(f"/flags/{project_key}/{flag_key}")

        links = {}
        if "_links" in flag:
            for link_name, link_data in flag["_links"].items():
                links[link_name] = link_data.get("href")

        # Extract environment statuses
        environments = {}
        if "environments" in flag:
            for env_key, env_data in flag["environments"].items():
                # Extract variations if available
                variations = []
                if "variations" in env_data:
                    for variation in env_data.get("variations", []):
                        variations.append({
                            "value": variation.get("value"),
                            "description": variation.get("description")
                        })

                environments[env_key] = {
                    "on": env_data.get("on", False),
                    "last_modified": env_data.get("lastModified"),
                    "version": env_data.get("version"),
                    "targeting_enabled": env_data.get("trackEvents", False),
                    "variations": variations,
                    "prerequisites": env_data.get("prerequisites", [])
                }

        return {
            "key": flag.get("key"),
            "name": flag.get("name"),
            "description": flag.get("description"),
            "tags": flag.get("tags", []),
            "variation_type": flag.get("variationType"),
            "temporary": flag.get("temporary", False),
            "client_side_availability": flag.get("clientSideAvailability"),
            "created_date": flag.get("creationDate"),
            "environments": environments,
            "links": links
        }

    @action(description="Gets flag status for a given `user_key` in the given `flag_key`")
    def get_flag_status_for_user(self, flag_key: str, user_key: str, user_attrs: Dict = None) -> Dict:
        """
        Evaluate a feature flag for a specific user using the SDK.

        Args:
            project_key: Project key identifier (not used with SDK but kept for API consistency)
            environment_key: Environment key identifier (not used with SDK but kept for API consistency)
            flag_key: Feature flag key identifier
            user_key: User identifier
            user_attrs: Optional dictionary of additional user attributes for targeting

        Returns:
            Flag evaluation result
        """
        if not self.ld_client:
            raise ValueError("SDK key is required for user-specific flag evaluation")

        try:
            # Create a context for the user (replaces the deprecated user object)
            user_context = Context.builder(user_key).kind('user')

            # Add any additional attributes if provided
            if user_attrs:
                for key, value in user_attrs.items():
                    if key != 'key':  # Skip the key as it's already set
                        user_context.set(key, value)

            # Build the context
            context = user_context.build()

            # Evaluate the flag for this context
            flag_value = self.ld_client.variation(flag_key, context, None)

            # Get additional evaluation details if available
            evaluation_detail = self.ld_client.variation_detail(flag_key, context, None)

            return {
                "flag_key": flag_key,
                "user_key": user_key,
                "value": flag_value,
                "variation_index": evaluation_detail.variation_index,
                "reason": evaluation_detail.reason
            }

        except Exception as e:
            self.logger.error(f"Error evaluating flag for user {user_key}: {e}")
            return {
                "flag_key": flag_key,
                "user_key": user_key,
                "error": str(e),
                "value": None
            }

    @action(description="Gets status given `flag_key` for all users `user_key` in the give `project_key` and `environment_key`")
    def get_flag_status_for_all_users(self, project_key: str, environment_key: str,
                                      flag_key: str) -> Dict:
        """
        Get the status of a feature flag across all users.

        Args:
            project_key: Project key identifier
            environment_key: Environment key identifier
            flag_key: Feature flag key identifier

        Returns:
            Dictionary with flag status information
        """
        try:
            # Get the flag configuration from the API
            response = self._make_request(f"/flags/{project_key}/{flag_key}")

            # Extract variations for easier reference
            variations = []
            for i, variation in enumerate(response.get("variations", [])):
                variations.append({
                    "index": i,
                    "value": variation.get("value"),
                    "description": variation.get("description")
                })

            # Create a simplified response
            result = {
                "flag_key": flag_key,
                "environment": environment_key,
                "status": "on" if response.get("on", False) else "off",
                "variations": variations,
                "default_rule": response.get("fallthrough"),
                "off_variation": {
                    "index": response.get("offVariation"),
                    "value": variations[response.get("offVariation")]["value"] if variations and response.get(
                        "offVariation") is not None else None
                } if response.get("offVariation") is not None else None
            }

            # Add targeting rules information if available
            if "rules" in response:
                result["rules"] = [
                    {
                        "id": rule.get("_id"),
                        "description": rule.get("description", ""),
                        "variation": rule.get("variation")
                    }
                    for rule in response.get("rules", [])
                ]

            return result

        except Exception as e:
            self.logger.error(f"Error getting flag status: {e}")
            return {
                "flag_key": flag_key,
                "environment": environment_key,
                "error": str(e)
            }

    @action(description="Get evaluations of all flags for a specific context using the SDK, Returns: Dictionary with flag evaluations")
    def get_flag_evaluations_for_context(self, project_key: str,
                                         context_kind: str, context_key: str,
                                         context_attrs: Dict = None) -> Dict:
        """
        Get evaluations of all flags for a specific context using the SDK.

        Args:
            project_key: Project key identifier (used to filter flags if needed)
            environment_key: Environment key identifier (not used with SDK)
            context_kind: Context kind (e.g., "user")
            context_key: Context identifier
            context_attrs: Optional dictionary of additional context attributes

        Returns:
            Dictionary with flag evaluations
        """
        if not self.ld_client:
            raise ValueError("SDK key is required for context-based flag evaluation")

        try:
            # Build the context
            context_builder = Context.builder(context_key).kind(context_kind)

            # Add any additional attributes if provided
            if context_attrs:
                for key, value in context_attrs.items():
                    if key != 'key':  # Skip the key as it's already set
                        context_builder.set(key, value)

            context = context_builder.build()

            # First, get all flags for the project to evaluate
            all_flags = self.list_feature_flags(project_key)
            flag_keys = [flag.get("key") for flag in all_flags]

            # Evaluate each flag for this context
            evaluations = []
            for flag_key in flag_keys:
                value = self.ld_client.variation(flag_key, context, None)
                detail = self.ld_client.variation_detail(flag_key, context, None)

                evaluations.append({
                    "flag_key": flag_key,
                    "value": value,
                    "variation_index": detail.variation_index,
                    "reason": detail.reason
                })

            return {
                "context_key": context_key,
                "context_kind": context_kind,
                "project": project_key,
                "evaluations": evaluations
            }

        except Exception as e:
            self.logger.error(f"Error getting flag evaluations for context {context_key}: {e}")
            return {
                "context_key": context_key,
                "context_kind": context_kind,
                "error": str(e)
            }

    @action(description="Search for feature flags with specific tags and return simplified results. Returns: List of simplified matching feature flags")
    def search_flags_by_tags(self, tags: List[str], project_key: Optional[str] = None) -> List[Dict]:
        """
        Search for feature flags with specific tags and return simplified results.

        Args:
            tags: List of tag strings to search for
            project_key: Optional project key to limit search

        Returns:
            List of simplified matching feature flags
        """
        endpoint = "/flags"
        params = {"tag": tags}

        if project_key:
            endpoint = f"/flags/{project_key}"

        response = self._make_request(endpoint, params=params)

        simplified_flags = []
        for flag in response.get("items", []):
            simplified_flags.append({
                "key": flag.get("key"),
                "name": flag.get("name"),
                "description": flag.get("description"),
                "tags": flag.get("tags", []),
                "variation_type": flag.get("variationType"),
                "temporary": flag.get("temporary", False)
            })

        return simplified_flags

    @action(description="Get recent changes to a feature flag. Returns: List of flag changes with timestamps and values")
    def get_flag_audit_history(self, flag_key: str, lookback_hours: int = 24) -> List[Dict]:
        """
        Get recent changes to a feature flag.

        Args:
            flag_key: The feature flag to investigate
            lookback_hours: How far back to look for changes

        Returns:
            List of flag changes with timestamps and values
        """
        try:
            # Calculate timestamp for lookback period
            since_time = int((datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000)

            # Fetch audit log entries for this flag
            endpoint = f"/auditlog"
            params = {
                "q": flag_key,
                "after": since_time,
                "limit": 20
            }

            response = self._make_request(endpoint, params=params)
            # Process and format the audit log entries
            audit_entries = []
            for entry in response.get("items", []):
                audit_entries.append({
                    "timestamp": entry.get("date"),
                    "actor": entry.get("member", {}).get("email"),
                    "action": entry.get("titleVerb"),
                    "changes": entry.get("changes", []),
                    "description": entry.get("description")
                })

            return audit_entries

        except Exception as e:
            print(f"Error fetching audit history: {e}")
            return []

    @action(description="Search for feature flags by name. Returns: List of matching feature flags")
    def search_flags_by_name(self, query: str, project_key: Optional[str] = None) -> List[Dict]:
        """
        Search for feature flags by name.

        Args:
            query: Search query string
            project_key: Optional project key to limit search

        Returns:
            List of matching feature flags
        """
        endpoint = "/flags"
        params = {"query": query}

        if project_key:
            endpoint = f"/flags/{project_key}"

        response = self._make_request(endpoint, params=params)
        return response.get("items", [])

    @action(description="Get details about a feature flag, including what context is expected. Returns: Details about the feature flag including context requirements")
    def get_feature_flag_context_details(self, project_key, flag_key):
        """
        Get details about a feature flag, including what context is expected.

        Args:
            project_key (str): The LaunchDarkly project key
            flag_key (str): The key of the feature flag to inspect

        Returns:
            dict: Details about the feature flag including context requirements
        """
        context_info = {
            "flag_key": flag_key,
            "required_context_kinds": [],
            "context_attributes_used": []
        }

        try:
            # 1. First get the flag details
            endpoint = f"/flags/{project_key}/{flag_key}"
            flag_data = self._make_request(endpoint)

            # Store basic flag info
            context_info["flag_name"] = flag_data.get("name", "")
            context_info["description"] = flag_data.get("description", "")

            # 2. Get all environments to check each one
            environments = flag_data.get("environments", {})

            for env_key, env_data in environments.items():
                # Extract prerequisite flags if any
                if "prerequisites" in env_data and env_data["prerequisites"]:
                    if "prerequisites" not in context_info:
                        context_info["prerequisites"] = []
                    for prereq in env_data["prerequisites"]:
                        context_info["prerequisites"].append({
                            "flag_key": prereq.get("key"),
                            "variation": prereq.get("variation")
                        })

                # Check targeting rules for context kinds and attributes
                if "rules" in env_data:
                    for rule in env_data["rules"]:
                        if "clauses" in rule:
                            for clause in rule["clauses"]:
                                # The new context-based LaunchDarkly API uses "contextKind" instead of "userType"
                                context_kind = clause.get("contextKind")
                                if context_kind and context_kind not in context_info["required_context_kinds"]:
                                    context_info["required_context_kinds"].append(context_kind)

                                attribute = clause.get("attribute")
                                if attribute and attribute not in context_info["context_attributes_used"]:
                                    context_info["context_attributes_used"].append(attribute)

                # Check targets for context kinds
                if "targets" in env_data:
                    for target in env_data["targets"]:
                        context_kind = target.get("contextKind")
                        if context_kind and context_kind not in context_info["required_context_kinds"]:
                            context_info["required_context_kinds"].append(context_kind)

                # The fallthrough might also contain clauses with context requirements
                if "fallthrough" in env_data and "clauses" in env_data["fallthrough"]:
                    for clause in env_data["fallthrough"]["clauses"]:
                        context_kind = clause.get("contextKind")
                        if context_kind and context_kind not in context_info["required_context_kinds"]:
                            context_info["required_context_kinds"].append(context_kind)

                        attribute = clause.get("attribute")
                        if attribute and attribute not in context_info["context_attributes_used"]:
                            context_info["context_attributes_used"].append(attribute)
            return context_info

        except requests.exceptions.RequestException as e:
            return {"error": f"Failed to fetch flag details: {str(e)}"}

    @action(description="Detect potential issues with feature flags for a specific user. Returns: List of potential flag issues")
    def detect_flag_issues(self, project_key: str, environment_key: str,
                           user_key: str, expected_flags: Dict[str, Any] = None) -> List[FlagIssue]:
        """
        Detect potential issues with feature flags for a specific user.

        Args:
            project_key: Project key identifier
            environment_key: Environment key identifier
            user_key: User identifier
            expected_flags: Dictionary mapping flag keys to expected values (optional)

        Returns:
            List of potential flag issues
        """
        issues = []

        try:
            # Get all flags for the user
            flag_evaluations = self.get_flag_evaluations_for_context(project_key, "user", user_key)

            if flag_evaluations:
                flag_evaluations = flag_evaluations.get("evaluations", [])
            # If expected flag values were provided, check for discrepancies
            if expected_flags:
                for flag_key, expected_value in expected_flags.items():
                    found = False
                    for evaluation in flag_evaluations:
                        if evaluation.get("flag_key") == flag_key:
                            found = True
                            actual_value = evaluation.get("value")

                            if actual_value != expected_value:
                                issues.append(FlagIssue(
                                    flag_key=flag_key,
                                    project_key=project_key,
                                    environment_key=environment_key,
                                    user_key=user_key,
                                    expected_value=expected_value,
                                    actual_value=actual_value,
                                    issue_type="wrong_value",
                                    description=f"Flag '{flag_key}' has value '{actual_value}' instead of expected '{expected_value}'"
                                ))

                            break

                    if not found:
                        issues.append(FlagIssue(
                            flag_key=flag_key,
                            project_key=project_key,
                            environment_key=environment_key,
                            user_key=user_key,
                            expected_value=expected_value,
                            actual_value=None,
                            issue_type="flag_not_found",
                            description=f"Flag '{flag_key}' was not found for user '{user_key}'"
                        ))

            flags = self.list_feature_flags(project_key)

            for flag in flags:
                flag_key = flag.get("key")
                # Look for flags that were recently changed and are active for this user
                recent_changes = self.get_flag_audit_history(flag_key, 24)
                # Check if this recently changed flag is active for the user
                for evaluation in flag_evaluations:
                    eval_key = evaluation.get("flag_key")
                    if flag_key == eval_key and evaluation.get("value") is True:
                        issues.append(FlagIssue(
                            flag_key=flag_key,
                            project_key=project_key,
                            environment_key=environment_key,
                            user_key=user_key,
                            expected_value=None,
                            actual_value=True,
                            issue_type="recently_enabled",
                            description=f"Flag '{flag_key}' was recently changed and is enabled for user '{user_key}'"
                        ))
                        break

        except Exception as e:
            self.logger.error(f"Error detecting flag issues: {e}")

        return issues

    @action(description="Find probable feature flag causes for an incident based on affected users. Returns: List of potential flag issues that might explain the incident")
    def find_probable_flag_cause(self, project_key: str, environment_key: str, affected_users: List[str]) -> List[FlagIssue]:
        """
        Find probable feature flag causes for an incident based on affected users.

        Args:
            project_key: Project key identifier
            environment_key: Environment key identifier
            affected_users: List of affected user keys

        Returns:
            List of potential flag issues that might explain the incident
        """
        all_issues = []

        # Get flags for all affected users
        for user_key in affected_users:
            issues = self.detect_flag_issues(project_key, environment_key, user_key)
            all_issues.extend(issues)

        # Group and rank issues by frequency and type
        issue_count = {}
        for issue in all_issues:
            key = (issue.flag_key, issue.issue_type)
            if key in issue_count:
                issue_count[key] += 1
            else:
                issue_count[key] = 1

        # Sort issues by frequency
        ranked_issues = sorted(
            all_issues,
            key=lambda x: issue_count.get((x.flag_key, x.issue_type), 0),
            reverse=True
        )

        # Remove duplicates while preserving order
        unique_issues = []
        seen = set()
        for issue in ranked_issues:
            key = (issue.flag_key, issue.issue_type)
            if key not in seen:
                seen.add(key)
                unique_issues.append(issue)

        return unique_issues

    # def available_actions(self) -> str:
    #     return """
    #         - list_repositories: Lists all available repositories
    #         - get_repository: Gets details about a specific repository (params: org_name, repo_name)
    #         - clone_repository: Clones a repository locally (params: org_name, repo_name)
    #         - list_files: Lists files in a repository (params: org_name, repo_name, path)
    #         - read_file: Reads a file's content (params: org_name, repo_name, file_path)
    #         - get_recent_commits: Gets recent commits (params: org_name, repo_name, limit)
    #         - get_commit_diff: Gets diff for a commit (params: org_name, repo_name, commit_hash)
    #         - search_code: Searches for code (params: org_name, query)
    #         - get_file_content: Gets file content via API (params: org_name, repo_name, file_path, ref)
    #     """
    #
    # def execute_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
    #     """Execute a specific action based on the agent's decision."""
    #     try:
    #         project_key = params.get("project_key")
    #         environment_key = params.get("environment_key")
    #         flag_key = params.get("flag_key")
    #
    #         if not project_key and action not in ["search_flags_by_name", "get_flag_audit_history",  "list_projects"]:
    #             return {"status": "error", "message": f"`project_key` is required for {action} action"}
    #
    #         if action == "list_projects":
    #             return {"status": "success", "data": self.list_projects()}
    #
    #         elif action == "list_environments":
    #             return {"status": "success", "data": self.list_environments(project_key)}
    #
    #         elif action == "list_feature_flags":
    #             return {"status": "success", "data": self.list_feature_flags(project_key)}
    #
    #         elif action == "get_flag_status_for_all_users":
    #             if not flag_key or environment_key:
    #                 return {"status": "error", "message": "`flag_key` and `environment_key` are required for get_flag_status_for_all_users action"}
    #             return {"status": "success", "data": self.get_flag_status_for_all_users(project_key, environment_key, flag_key)}
    #
    #         elif action == "get_feature_flag":
    #             if not flag_key:
    #                 return {"status": "error", "message": "`flag_key` is required for get_feature_flag action"}
    #             return {"status": "success", "data": self.get_feature_flag(project_key, flag_key)}
    #
    #         elif action == "get_flag_status_for_user":
    #             user_key = params.get("user_key")
    #             if not user_key:
    #                 return {"status": "error", "message": "`user_key` is required for get_flag_status_for_user action"}
    #             return {"status": "success", "data": self.get_flag_status_for_user(project_key, user_key)}
    #
    #         elif action == "search_flags_by_tags":
    #             tags = params.get("tags", [])
    #             if not tags or len(tags) == 0:
    #                 return {"status": "error", "message": "Atleast one `tags` is required for search_flags_by_tags action"}
    #             return {"status": "success", "data": self.search_flags_by_tags(tags, project_key)}
    #
    #         elif action == "get_flag_audit_history":
    #             if not flag_key:
    #                 return {"status": "error", "message": "`flag_key` is required for get_flag_audit_history action"}
    #             return {"status": "success", "data": self.get_flag_audit_history(flag_key)}
    #
    #         elif action == "get_feature_flag_context_details":
    #             if not flag_key:
    #                 return {"status": "error", "message": "`flag_key` is required for get_feature_flag_context_details action"}
    #             return {"status": "success", "data": self.get_feature_flag_context_details(project_key, flag_key)}
    #
    #         elif action == "search_flags_by_name":
    #             query = params.get("query")
    #             if not query or not flag_key:
    #                 return {"status": "error", "message": "`flag_key` and `query` are required for search_flags_by_name action"}
    #             return {"status": "success", "data": self.search_flags_by_name(query, flag_key)}
    #
    #         elif action == "get_flag_evaluations_for_context":
    #             context_kind = params.get("context_kind")
    #             context_key = params.get("context_key")
    #             context_attr = params.get("context_attr")
    #             if not context_kind or not context_key:
    #                 return {"status": "error", "message": "`context_kind` and `context_key` are required for get_flag_evaluations_for_context action"}
    #             if context_attr and type(context_attr) is not Dict:
    #                 return {"status": "error", "message": "`context_attr` is expected to be type `Dict` for get_flag_evaluations_for_context action"}
    #             return {"status": "success", "data": self.get_flag_evaluations_for_context(project_key, context_kind, context_key, context_attr)}
    #
    #         else:
    #             return {"status": "error", "message": f"Unknown action: {action}"}
    #
    #     except Exception as e:
    #         return {"status": "error", "message": str(e)}

# if __name__ == "__main__":
#     # Initialize client
#     ld_client = LaunchDarklyClient(os.getenv("LAUNCHDARKLY_API_KEY"), os.getenv("LAUNCHDARKLY_SDK_KEY"))
#
#     print("=====================================================================")
#     print(f"List all projects: {ld_client.list_projects()}")
#     print("=====================================================================")
#     print(f"List all environments: {ld_client.list_environments("default")}")
#     print("=====================================================================")
#     print(f"List all feature flags: {ld_client.list_feature_flags("default")}")
#     print("=====================================================================")
#     print(f"Flag status for all users: {ld_client.get_flag_status_for_all_users("default", "production", "sample-feature")}")
#     print("=====================================================================")
#     print(f"Feature flag details: {ld_client.get_feature_flag("default", "sample-feature")}")
#     print("=====================================================================")
#     print(f"Flag status for particular user: {ld_client.get_flag_status_for_user("sample-feature", "Aayush")}")
#     print("=====================================================================")
#     print(f"Search by tags: {ld_client.search_flags_by_tags(["flag"], "default")}")
#     print("=====================================================================")
#     print(f"Recent flag changes: {ld_client.get_flag_audit_history("sample-feature")}")
#     print("=====================================================================")
#     print(f"Search by name: {ld_client.search_flags_by_name("1", "default")}")
#     print("=====================================================================")
#     print(f"Evaluation: {ld_client.get_flag_evaluations_for_context("default", "user", "Aayush")}")
#     print("=====================================================================")
#     print(f"Flag context: {ld_client.get_feature_flag_context_details("default", "sample-feature")}")
#     print("=====================================================================")
#     print(f"Detect issues: {ld_client.detect_flag_issues("default", "production", "Aayush", {"flag-1": "Karan"})}")
#     print("=====================================================================")
#     print(f"Find probable cause: {ld_client.find_probable_flag_cause("default", "production", ["Karan", "Aayush"])}")
#     print("=====================================================================")
