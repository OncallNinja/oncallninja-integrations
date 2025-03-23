import requests
import urllib.parse
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
from .action_router import action, ActionRouter

class SentryAPIClient(ActionRouter):
    """
    Client for interacting with the Sentry API.
    Supports read operations for projects, issues, events, and more.
    """

    BASE_URL = "https://sentry.io/api/0/"

    def __init__(self, auth_token: str, organization_slug: str):
        """
        Initialize the Sentry API client.

        Args:
            auth_token: Sentry API authentication token
            organization_slug: The slug of your Sentry organization
        """
        self.auth_token = auth_token
        self.organization_slug = organization_slug
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

        super().__init__()

    @action(description='make HTTP request')
    def _make_request(self, endpoint: str, method: str = "GET", params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the Sentry API.

        Args:
            endpoint: API endpoint to call
            method: HTTP method (default: GET)
            params: Optional query parameters

        Returns:
            Response data as dictionary
        """
        url = urllib.parse.urljoin(self.BASE_URL, endpoint)

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params
            )

            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Print additional details for debugging
            print(f"Request failed: {e}")
            print(f"URL: {response.url}")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            raise

    # Organization endpoints
    @action(description='Get details about the specific organization')
    def get_organization(self) -> Dict:
        """Get details about the configured organization."""
        return self._make_request(f"organizations/{self.organization_slug}/")

    @action(description='Get all organizations available to user')
    def get_organizations(self) -> List[Dict]:
        """Get all organizations the user has access to."""
        return self._make_request("organizations/")

    @action(description='Get organization statistics')
    def get_organization_stats(self, stat: str = "received", since: Optional[datetime] = None) -> Dict:
        """
        Get organization stats like events received or rejected.

        Args:
            stat: Stat to query (received, rejected, blacklisted)
            since: Starting date for stats

        Returns:
            Organization statistics
        """
        params = {"stat": stat}
        if since:
            params["since"] = since.isoformat()

        return self._make_request(f"organizations/{self.organization_slug}/stats/", params=params)

    # Project endpoints

    @action(description='Get all projects in the organization slug')
    def get_projects(self) -> List[Dict]:
        """Get all projects in the organization."""
        return self._make_request(f"organizations/{self.organization_slug}/projects/")

    @action(description='Get details for a project')
    def get_project(self, project_slug: str) -> Dict:
        """
        Get details for a specific project.

        Args:
            project_slug: The slug of the project

        Returns:
            Project details
        """
        return self._make_request(f"projects/{self.organization_slug}/{project_slug}/")

    @action(description='Get project statistics')
    def get_project_stats(self, project_slug: str, stat: str = "received", since: Optional[datetime] = None) -> Dict:
        """
        Get statistics for a specific project.

        Args:
            project_slug: The slug of the project
            stat: Stat to query (received, rejected, blacklisted)
            since: Starting date for stats

        Returns:
            Project statistics
        """
        params = {"stat": stat}
        if since:
            params["since"] = since.isoformat()

        return self._make_request(
            f"projects/{self.organization_slug}/{project_slug}/stats/",
            params=params
        )

    @action(description='get all client keys in project')
    def get_project_keys(self, project_slug: str) -> List[Dict]:
        """
        Get all client keys for a project.

        Args:
            project_slug: The slug of the project

        Returns:
            List of client keys
        """
        return self._make_request(f"projects/{self.organization_slug}/{project_slug}/keys/")

    # Issues endpoints

    @action(description='Get issues')
    def get_issues(
            self,
            project_slug: Optional[str] = None,
            query: Optional[str] = None,
            status: Optional[str] = None,
            environment: Optional[str] = None,
            limit: int = 100,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            sort_by: str = "date"
    ) -> List[Dict]:
        """
        Get issues (groups of events) in the organization or project with timestamp filtering.

        Args:
            project_slug: Optional project slug to filter by
            query: Search query
            status: Filter by status (resolved, unresolved, ignored)
            environment: Filter by environment
            limit: Maximum number of issues to return
            start_date: Start datetime for issue filtering (when issues occurred)
            end_date: End datetime for issue filtering
            sort_by: Sort results by field (date, new, priority, freq, user)

        Returns:
            List of issues with timestamps
        """
        params = {"limit": limit}

        # Handle sorting - Sentry API uses "-" prefix for descending
        # if sort_by:
        #     # Default to descending order
        #     params["sort"] = f"-{sort_by}"

        if query:
            params["query"] = query
        if status:
            params["status"] = status
        if environment:
            params["environment"] = environment

        # Handle date parameters
        if start_date and end_date:
            params["start"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            params["end"] = end_date.strftime("%Y-%m-%dT%H:%M:%S")
        elif start_date:
            params["start"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        elif end_date:
            params["end"] = end_date.strftime("%Y-%m-%dT%H:%M:%S")

        if project_slug:
            endpoint = f"projects/{self.organization_slug}/{project_slug}/issues/"
        else:
            endpoint = f"organizations/{self.organization_slug}/issues/"

        return self._make_request(endpoint, params=params)

    @action(description='Get issues with timestamps')
    def get_issues_with_timestamps(
            self,
            project_slug: Optional[str] = None,
            days_back: int = 14,
            status: str = "unresolved",
            limit: int = 100
    ) -> List[Dict]:
        """
        Convenience method to get issues with timestamps.
        Returns all issue timestamps including:
        - dateCreated: When the issue was first seen
        - lastSeen: When the issue was last seen
        - firstSeen: First occurrence of this issue

        Args:
            project_slug: Optional project slug to filter by
            days_back: Number of days to look back
            status: Filter by status (resolved, unresolved, ignored)
            limit: Maximum number of issues to return

        Returns:
            List of issues with full timestamp data
        """
        # Use statsPeriod instead of start/end dates when possible
        params = {
            "statsPeriod": f"{days_back}d",
            "limit": limit
        }

        if status:
            params["status"] = status

        if project_slug:
            endpoint = f"projects/{self.organization_slug}/{project_slug}/issues/"
        else:
            endpoint = f"organizations/{self.organization_slug}/issues/"

        return self._make_request(endpoint, params=params)

    @action(description='Get a particular issue')
    def get_issue(self, issue_id: str) -> Dict:
        """
        Get details for a specific issue.

        Args:
            issue_id: ID of the issue

        Returns:
            Issue details including timestamps
        """
        return self._make_request(f"issues/{issue_id}/")

    @action(description='Get events related to an issue')
    def get_issue_events(
            self,
            issue_id: str,
            limit: int = 100,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Get events for a specific issue with optional time filtering.

        Args:
            issue_id: ID of the issue
            limit: Maximum number of events to return
            start_date: Optional start datetime for filtering
            end_date: Optional end datetime for filtering

        Returns:
            List of events with timestamps
        """
        params = {"limit": limit}

        if start_date and end_date:
            params["statsPeriod"] = ""
            params["start"] = start_date.strftime("%Y-%m-%d")
            params["end"] = end_date.strftime("%Y-%m-%d")
        elif start_date:
            params["statsPeriod"] = ""
            params["start"] = start_date.strftime("%Y-%m-%d")
        elif end_date:
            params["statsPeriod"] = ""
            params["end"] = end_date.strftime("%Y-%m-%d")

        return self._make_request(f"issues/{issue_id}/events/", params=params)

    @action(description='Get tags for an issue')
    def get_issue_tags(self, issue_id: str) -> List[Dict]:
        """
        Get tags for a specific issue.

        Args:
            issue_id: ID of the issue

        Returns:
            List of tags
        """
        return self._make_request(f"issues/{issue_id}/tags/")

    # Event endpoints
    @action(description='Get a particular event')
    def get_event(self, project_slug: str, event_id: str) -> Dict:
        """
        Get details for a specific event.

        Args:
            project_slug: Slug of the project
            event_id: ID of the event

        Returns:
            Event details including timestamp
        """
        return self._make_request(f"projects/{self.organization_slug}/{project_slug}/events/{event_id}/")

    @action(description='Get events for specific project with timestamp filtering')
    def get_project_events(
            self,
            project_slug: str,
            query: Optional[str] = None,
            environment: Optional[str] = None,
            limit: int = 100,
            days_back: Optional[int] = None
    ) -> List[Dict]:
        """
        Get events for a specific project with timestamp filtering.

        Args:
            project_slug: Slug of the project
            query: Search query
            environment: Filter by environment
            limit: Maximum number of events to return
            days_back: Number of days to look back

        Returns:
            List of events with timestamps
        """
        params = {"limit": limit}

        if query:
            params["query"] = query
        if environment:
            params["environment"] = environment
        if days_back:
            params["statsPeriod"] = f"{days_back}d"

        return self._make_request(
            f"projects/{self.organization_slug}/{project_slug}/events/",
            params=params
        )

    # Release endpoints
    @action(description='Get all releases for the organization or project with optional time filtering.')
    def get_releases(
            self,
            project_slug: Optional[str] = None,
            days_back: Optional[int] = None
    ) -> List[Dict]:
        """
        Get all releases for the organization or project with optional time filtering.

        Args:
            project_slug: Optional project slug to filter by
            days_back: Number of days to look back

        Returns:
            List of releases with timestamps
        """
        params = {}

        if days_back:
            params["statsPeriod"] = f"{days_back}d"

        if project_slug:
            endpoint = f"projects/{self.organization_slug}/{project_slug}/releases/"
        else:
            endpoint = f"organizations/{self.organization_slug}/releases/"

        return self._make_request(endpoint, params=params)

    @action(description='Get details for a specific release')
    def get_release(self, version: str, project_slug: Optional[str] = None) -> Dict:
        """
        Get details for a specific release.

        Args:
            version: Release version
            project_slug: Optional project slug

        Returns:
            Release details including timestamps
        """
        if project_slug:
            endpoint = f"projects/{self.organization_slug}/{project_slug}/releases/{urllib.parse.quote(version)}/"
        else:
            endpoint = f"organizations/{self.organization_slug}/releases/{urllib.parse.quote(version)}/"

        return self._make_request(endpoint)

    @action(description='Get files for a specific release')
    def get_release_files(self, version: str, project_slug: Optional[str] = None) -> List[Dict]:
        """
        Get files for a specific release.

        Args:
            version: Release version
            project_slug: Optional project slug

        Returns:
            List of release files
        """
        if project_slug:
            endpoint = f"projects/{self.organization_slug}/{project_slug}/releases/{urllib.parse.quote(version)}/files/"
        else:
            endpoint = f"organizations/{self.organization_slug}/releases/{urllib.parse.quote(version)}/files/"

        return self._make_request(endpoint)

    # Team endpoints
    @action(description='Get all teams in the organization')
    def get_teams(self) -> List[Dict]:
        """Get all teams in the organization."""
        return self._make_request(f"organizations/{self.organization_slug}/teams/")

    @action(description='Get details for a specific team')
    def get_team(self, team_slug: str) -> Dict:
        """
        Get details for a specific team.

        Args:
            team_slug: Slug of the team

        Returns:
            Team details
        """
        return self._make_request(f"teams/{self.organization_slug}/{team_slug}/")

    @action(description='Get projects for a specific team')
    def get_team_projects(self, team_slug: str) -> List[Dict]:
        """
        Get projects for a specific team.

        Args:
            team_slug: Slug of the team

        Returns:
            List of projects
        """
        return self._make_request(f"teams/{self.organization_slug}/{team_slug}/projects/")

    # Member endpoints
    @action(description='Get all members in the organization')
    def get_members(self) -> List[Dict]:
        """Get all members in the organization."""
        return self._make_request(f"organizations/{self.organization_slug}/members/")

    @action(description='Get details for a specific member')
    def get_member(self, member_id: str) -> Dict:
        """
        Get details for a specific member.

        Args:
            member_id: ID of the member

        Returns:
            Member details
        """
        return self._make_request(f"organizations/{self.organization_slug}/members/{member_id}/")

    @action(description='Get the stack trace from a specific issue by finding its events and extracting stack trace data')
    def get_stack_trace_from_issue(self, issue_id: str) -> Optional[Dict]:
        """
        Get the stack trace for a specific issue by finding its events and extracting stack trace data.

        Args:
            issue_id: ID of the Sentry issue

        Returns:
            Dictionary containing the stack trace frames or None if not found
        """
        try:
            # First get the issue details to check the project
            issue_details = self.get_issue(issue_id)

            # Get the project from the issue
            project_id = None
            if issue_details and "project" in issue_details:
                if isinstance(issue_details["project"], dict):
                    project_id = issue_details["project"].get("id")
                else:
                    project_id = issue_details["project"]

            # Find the project slug using the project ID
            project_slug = None
            if project_id:
                projects = self.get_projects()
                for project in projects:
                    if str(project.get("id")) == str(project_id):
                        project_slug = project.get("slug")
                        break

            if not project_slug:
                print(f"Could not determine project slug for issue {issue_id}")
                return None

            # Get events for the issue
            events = self.get_issue_events(issue_id, limit=1)
            if not events:
                print(f"No events found for issue {issue_id}")
                return None

            # Use the first (most recent) event
            event_id = events[0].get("id")
            if not event_id:
                print("Event ID not found in the response")
                return None

            # Get complete event data
            event_data = self.get_event(project_slug, event_id)

            # Extract stack trace from the event data
            if "entries" in event_data:
                for entry in event_data["entries"]:
                    # Check if this entry is a stack trace
                    if entry.get("type") == "stacktrace":
                        # Return the full stack trace data
                        return {
                            "frames": entry["data"].get("frames", []),
                            "event_id": event_id,
                            "project_slug": project_slug
                        }

                    # Check if stack trace is in exception data
                    if entry.get("type") == "exception" and "data" in entry:
                        exception_data = entry["data"]
                        if "values" in exception_data:
                            for exception in exception_data["values"]:
                                if "stacktrace" in exception:
                                    return {
                                        "frames": exception["stacktrace"].get("frames", []),
                                        "event_id": event_id,
                                        "project_slug": project_slug,
                                        "exception_type": exception.get("type"),
                                        "exception_value": exception.get("value")
                                    }

            # If we get here, we couldn't find a stack trace in the standard locations
            print(f"No stack trace found in event {event_id} for issue {issue_id}")
            return None

        except Exception as e:
            print(f"Error extracting stack trace: {e}")
            return None

    def format_stack_trace(self, stack_trace_data: Dict) -> str:
        """
        Format a stack trace into a readable string.

        Args:
            stack_trace_data: Stack trace data from get_stack_trace_from_issue

        Returns:
            Formatted stack trace string
        """
        if not stack_trace_data or "frames" not in stack_trace_data:
            return "No stack trace available"

        frames = stack_trace_data["frames"]

        # Build a formatted stack trace string
        formatted_trace = []
        
        # Add header information
        formatted_trace.append(f"Stack Trace for Event: {stack_trace_data.get('event_id')}")
        formatted_trace.append(f"Project: {stack_trace_data.get('project_slug')}")

        if "exception_type" in stack_trace_data and "exception_value" in stack_trace_data:
            formatted_trace.append(
                f"Exception: {stack_trace_data.get('exception_type')}: {stack_trace_data.get('exception_value')}\n")

        formatted_trace.append("Stack frames (most recent call last):")

        # Reverse the frames to show most recent call last (Python standard)
        for i, frame in enumerate(reversed(frames)):
            filename = frame.get("filename", "unknown")
            function = frame.get("function", "unknown")
            line_no = frame.get("lineNo", "?")
            col_no = frame.get("colNo", "?")
            
            # Add frame header with detailed information
            frame_str = f"\n{i + 1}. File: {filename}, Line: {line_no}, Column: {col_no}"
            frame_str += f"\n   Function: {function}"
            formatted_trace.append(frame_str)
            
            # Extract and format context if available
            context_lines = frame.get("context", [])
            if context_lines:
                formatted_trace.append("\n   Context:")
                
                # Find the error line to determine proper padding for line numbers
                max_line_num = max([line_num for line_num, _ in context_lines]) if context_lines else 0
                padding = len(str(max_line_num))
                
                # Format each context line with proper indentation and highlighting for the error line
                for line_num, code_line in context_lines:
                    # Determine if this is the error line
                    prefix = "-> " if line_num == line_no else "   "
                    # Format line with consistent padding for line numbers
                    formatted_line = f"   {prefix}{str(line_num).rjust(padding)}: {code_line}"
                    formatted_trace.append(formatted_line)

        return "\n".join(formatted_trace)

    def get_formatted_stack_trace(self, issue_id: str) -> str:
        """
        Get a formatted stack trace for a specific issue.

        Args:
            issue_id: ID of the Sentry issue

        Returns:
            Formatted stack trace string
        """
        stack_trace_data = self.get_stack_trace_from_issue(issue_id)
        if not stack_trace_data:
            return "No stack trace found for this issue."

        return self.format_stack_trace(stack_trace_data)

    @action(description='Get details for a specific issue')
    def get_issue_details(self, issue_id: str) -> Dict:
        """
        Get details for a specific issue, including aggregated data.

        Args:
            issue_id: ID of the issue

        Returns:
            Issue details including stack trace (if available)
        """
        return self._make_request(f"issues/{issue_id}/")



# if __name__=="__main__":
#     sentry_client = SentryAPIClient(auth_token="",
#                                     organization_slug="oncallninja")
#
#     from datetime import datetime, timedelta
#
#     last_week = datetime.now() - timedelta(days=7)
#
#     # issues = sentry_client.get_issues_with_timestamps(
#     #     project_slug="python-gcpfunctions",
#     #     days_back=14  # Last 14 days
#     # )
#
#     from datetime import datetime, timedelta
#
#     start_date = datetime.now() - timedelta(hours=5)
#     end_date = datetime.now()
#
#     issues = sentry_client.get_issues(
#         project_slug="python-gcpfunctions",
#         start_date=start_date,
#         end_date=end_date,
#     )
#
#     print(issues)
#
#     issue_details = sentry_client.get_issue_details(issue_id='6377844533')
#
#     print(issue_details)
#
#     event = sentry_client.get_event(event_id='5f03ac6724dd42fdb9b9e301a1604e5c', project_slug='python-gcpfunctions')
#     print(event)
#
#     stack_trace = sentry_client.get_formatted_stack_trace(issue_id='6377844533')
#     print(stack_trace)
