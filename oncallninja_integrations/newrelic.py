from datetime import datetime
from typing import Optional, Dict, Union, List, Tuple

import requests

from oncallninja_integrations.action_router import ActionRouter, action


class NewRelicClient(ActionRouter):
    def __init__(
            self,
            account_id: str = None,
            api_key: str = None,
            region: str = "US"  # Can be "US" or "EU"
    ):
        """
        Initialize New Relic API interface

        Args:
            account_id: New Relic account ID
            api_key: New Relic API key for authentication
            region: New Relic region (US or EU)
        """
        self.account_id = account_id

        # Set up base URLs based on region
        if region.upper() == "EU":
            self.query_api_url = "https://api.eu.newrelic.com/graphql"
            self.rest_api_base_url = "https://api.eu.newrelic.com/v2"
            self.nrdb_url = "https://nrdb.eu.newrelic.com/v1"
        else:  # Default to US
            self.query_api_url = "https://api.newrelic.com/graphql"
            self.rest_api_base_url = "https://api.newrelic.com/v2"
            self.nrdb_url = "https://nrdb.newrelic.com/v1"

        # Set up headers for GraphQL API
        self.graphql_headers = {
            "Content-Type": "application/json",
            "API-Key": api_key
        }

        # Set up headers for REST API
        self.rest_headers = {
            "Content-Type": "application/json",
            "X-Api-Key": api_key
        }

        super().__init__()

    @action(description="NEWRELIC: Make GraphQL request to New Relic API.")
    def _make_graphql_request(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Make GraphQL request to New Relic API

        Args:
            query: GraphQL query string
            variables: Variables for the GraphQL query

        Returns:
            API response as dictionary
        """
        payload = {
            "query": query
        }

        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(
                url=self.query_api_url,
                headers=self.graphql_headers,
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"New Relic GraphQL API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise Exception(error_msg)

    @action(description="NEWRELIC: Make REST API request.")
    def _make_rest_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make REST API request to New Relic API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request payload

        Returns:
            API response as dictionary
        """
        url = f"{self.rest_api_base_url}/{endpoint.lstrip('/')}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.rest_headers,
                json=data
            )
            response.raise_for_status()
            return response.json() if response.text else {}
        except requests.exceptions.RequestException as e:
            error_msg = f"New Relic REST API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise Exception(error_msg)

    @action(description="NEWRELIC: Make NRDB query using NRQL.")
    def query_nrdb(self, nrql_query: str) -> Dict:
        """
        Query New Relic Database using NRQL

        Args:
            nrql_query: NRQL query string

        Returns:
            Query results
        """
        graphql_query = """
        query($accountId: Int!, $nrql: String!) {
            actor {
                account(id: $accountId) {
                    nrql(query: $nrql) {
                        results
                        metadata {
                            facets
                        }
                    }
                }
            }
        }
        """

        variables = {
            "accountId": int(self.account_id),
            "nrql": nrql_query
        }

        return self._make_graphql_request(graphql_query, variables)

    @action(description="NEWRELIC: Get logs with filtering options.")
    def get_logs(
            self,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            filters: Optional[Dict] = None,
            size: int = 100,
            sort_order: str = "DESC"
    ) -> Dict:
        """
        Get logs with various filtering options

        Args:
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            filters: Dictionary of field:value pairs to filter logs
            size: Number of results to return
            sort_order: Sort order ('ASC' or 'DESC')

        Returns:
            Dictionary containing matching logs
        """
        # Convert datetime objects to milliseconds since epoch if needed
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        elif isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time = int(dt.timestamp() * 1000)

        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        elif isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time = int(dt.timestamp() * 1000)

        # Build NRQL query
        nrql = f"SELECT * FROM Log WHERE timestamp >= {start_time} AND timestamp <= {end_time}"

        # Add filters if provided
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    value_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
                    nrql += f" AND {field} IN ({value_str})"
                elif isinstance(value, dict) and ("min" in value or "max" in value):
                    if "min" in value:
                        nrql += f" AND {field} >= {value['min']}"
                    if "max" in value:
                        nrql += f" AND {field} <= {value['max']}"
                elif isinstance(value, str):
                    nrql += f" AND {field} = '{value}'"
                else:
                    nrql += f" AND {field} = {value}"

        # Add sorting and limit
        nrql += f" ORDER BY timestamp {sort_order} LIMIT {size}"

        return self.query_nrdb(nrql)

    @action(description="NEWRELIC: Search logs by keyword within a time range.")
    def search_logs_by_keyword(
            self,
            keyword: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            size: int = 100,
            exact_match: bool = False
    ) -> Dict:
        """
        Search logs by keyword within a specified time range

        Args:
            keyword: Keyword to search for
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            size: Number of results to return
            exact_match: If True, perform an exact match search

        Returns:
            Dictionary containing matching logs
        """
        # Convert datetime objects to milliseconds since epoch if needed
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        elif isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time = int(dt.timestamp() * 1000)

        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        elif isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time = int(dt.timestamp() * 1000)

        # Build NRQL query for searching by keyword
        if exact_match:
            # Use '=' for exact matching
            nrql = f"SELECT * FROM Log WHERE timestamp >= {start_time} AND timestamp <= {end_time} AND message = '{keyword}'"
        else:
            # Use LIKE for partial matching
            nrql = f"SELECT * FROM Log WHERE timestamp >= {start_time} AND timestamp <= {end_time} AND message LIKE '%{keyword}%'"

        nrql += f" ORDER BY timestamp DESC LIMIT {size}"

        return self.query_nrdb(nrql)

    @action(description="NEWRELIC: Get distinct log levels.")
    def get_log_levels(self) -> List[str]:
        """
        Get distinct log levels from logs

        Returns:
            List of distinct log levels
        """
        nrql = "SELECT uniqueCount(level) FROM Log FACET level LIMIT 20"

        response = self.query_nrdb(nrql)

        # Extract log levels from response
        log_levels = []
        try:
            results = response.get("data", {}).get("actor", {}).get("account", {}).get("nrql", {}).get("results", [])
            log_levels = [entry.get("level") for entry in results if entry.get("level")]
        except (KeyError, AttributeError):
            pass

        return log_levels

    @action(description="NEWRELIC: Get available fields in logs.")
    def get_log_fields(self) -> List[str]:
        """
        Get available fields in New Relic logs

        Returns:
            List of available log fields
        """
        # Use keySet() in NRQL to find available attributes
        nrql = "SELECT keyset() FROM Log LIMIT 1"

        response = self.query_nrdb(nrql)

        # Extract fields from response
        fields = []
        try:
            results = response.get("data", {}).get("actor", {}).get("account", {}).get("nrql", {}).get("results", [])
            if results and len(results) > 0:
                # The first result will contain all attribute names
                fields = results[0].get("keys", [])
        except (KeyError, AttributeError):
            pass

        return sorted(fields)

    @action(description="NEWRELIC: Validate NRQL query.")
    def validate_nrql_query(self, nrql_query: str) -> Tuple[bool, Optional[Dict]]:
        """
        Validate NRQL query syntax

        Args:
            nrql_query: NRQL query string to validate

        Returns:
            Tuple of (is_valid, error_info)
        """
        # New Relic doesn't have a dedicated validation endpoint, so we'll try to execute with a small LIMIT
        # and catch any errors

        # Add LIMIT 1 if not already specified to minimize data transfer
        if "LIMIT" not in nrql_query.upper():
            nrql_query += " LIMIT 1"

        try:
            self.query_nrdb(nrql_query)
            return True, None
        except Exception as e:
            error_message = str(e)
            # Extract error details
            error_info = {"reason": error_message}
            return False, error_info

    @action(description="NEWRELIC: Get application list.")
    def get_applications(self) -> List[Dict]:
        """
        Get list of New Relic applications

        Returns:
            List of applications with their details
        """
        graphql_query = """
        query($accountId: Int!) {
            actor {
                account(id: $accountId) {
                    nrql(query: "SELECT uniques(appName) FROM Transaction") {
                        results
                    }
                }
            }
        }
        """

        variables = {
            "accountId": int(self.account_id)
        }

        response = self._make_graphql_request(graphql_query, variables)

        apps = []
        try:
            results = response.get("data", {}).get("actor", {}).get("account", {}).get("nrql", {}).get("results", [])
            for app in results:
                apps.append({"name": app.get("uniques.appName")})
        except (KeyError, AttributeError):
            pass

        return apps

    @action(description="NEWRELIC: Get entity information by GUID.")
    def get_entity(self, entity_guid: str) -> Dict:
        """
        Get information about a specific entity by GUID

        Args:
            entity_guid: New Relic entity GUID

        Returns:
            Entity information
        """
        graphql_query = """
        query($guid: EntityGuid!) {
            actor {
                entity(guid: $guid) {
                    guid
                    name
                    type
                    account {
                        id
                        name
                    }
                    domain
                    alertSeverity
                    tags {
                        key
                        values
                    }
                }
            }
        }
        """

        variables = {
            "guid": entity_guid
        }

        return self._make_graphql_request(graphql_query, variables)

    @action(description="NEWRELIC: Search entities.")
    def search_entities(self, name: str = None, domain: str = None, type: str = None, tags: List[Dict] = None) -> List[
        Dict]:
        """
        Search for New Relic entities

        Args:
            name: Entity name to search for
            domain: Entity domain (APM, BROWSER, INFRA, etc.)
            type: Entity type (APPLICATION, HOST, etc.)
            tags: List of tag conditions [{"key": "env", "value": "production"}]

        Returns:
            List of matching entities
        """
        # Build GraphQL query for entity search
        graphql_query = """
        query($accountId: Int!, $query: String) {
            actor {
                entitySearch(query: $query) {
                    results {
                        entities {
                            guid
                            name
                            type
                            domain
                            account {
                                id
                                name
                            }
                            tags {
                                key
                                values
                            }
                        }
                    }
                }
            }
        }
        """

        # Build query string
        query_parts = [f"accountId = {self.account_id}"]

        if name:
            query_parts.append(f"name = '{name}'")

        if domain:
            query_parts.append(f"domain = '{domain}'")

        if type:
            query_parts.append(f"type = '{type}'")

        if tags:
            for tag in tags:
                query_parts.append(f"tags.{tag['key']} = '{tag['value']}'")

        query = " AND ".join(query_parts)

        variables = {
            "accountId": int(self.account_id),
            "query": query
        }

        response = self._make_graphql_request(graphql_query, variables)

        entities = []
        try:
            results = response.get("data", {}).get("actor", {}).get("entitySearch", {}).get("results", {}).get(
                "entities", [])
            entities = results
        except (KeyError, AttributeError):
            pass

        return entities

    @action(description="NEWRELIC: Get metric data for an entity.")
    def get_metric_data(
            self,
            entity_guid: str,
            metric_name: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            timeseries: bool = True
    ) -> Dict:
        """
        Get metric data for a specific entity

        Args:
            entity_guid: Entity GUID
            metric_name: Name of the metric to retrieve
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            timeseries: If True, return timeseries data

        Returns:
            Metric data
        """
        # Convert datetime objects to ISO format if needed
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()

        graphql_query = """
        query($guid: EntityGuid!, $metricName: String!, $startTime: EpochMilliseconds!, $endTime: EpochMilliseconds!, $timeseries: Boolean!) {
            actor {
                entity(guid: $guid) {
                    ... on ApmApplicationEntity {
                        metrics {
                            timeseries(
                                metricName: $metricName,
                                startTime: $startTime,
                                endTime: $endTime
                            ) @include(if: $timeseries) {
                                startTime
                                endTime
                                timeslices {
                                    timestamp
                                    values {
                                        min
                                        max
                                        average
                                        sum
                                        count
                                    }
                                }
                            }
                            summary(
                                metricName: $metricName,
                                startTime: $startTime,
                                endTime: $endTime
                            ) {
                                min
                                max
                                average
                                sum
                                count
                            }
                        }
                    }
                }
            }
        }
        """

        # Convert ISO format to milliseconds since epoch
        if isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time_ms = int(dt.timestamp() * 1000)
        else:
            start_time_ms = start_time

        if isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time_ms = int(dt.timestamp() * 1000)
        else:
            end_time_ms = end_time

        variables = {
            "guid": entity_guid,
            "metricName": metric_name,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "timeseries": timeseries
        }

        return self._make_graphql_request(graphql_query, variables)

    @action(description="NEWRELIC: Get alerts for an account.")
    def get_alerts(self, only_open: bool = True) -> List[Dict]:
        """
        Get alerts for the account

        Args:
            only_open: If True, return only open alerts

        Returns:
            List of alerts
        """
        graphql_query = """
        query($accountId: Int!, $onlyOpen: Boolean!) {
            actor {
                account(id: $accountId) {
                    alerts {
                        violations(onlyOpen: $onlyOpen) {
                            results {
                                violationId
                                label
                                level
                                openedAt
                                closedAt
                                entity {
                                    guid
                                    name
                                }
                                conditionName
                                policyName
                            }
                        }
                    }
                }
            }
        }
        """

        variables = {
            "accountId": int(self.account_id),
            "onlyOpen": only_open
        }

        response = self._make_graphql_request(graphql_query, variables)

        alerts = []
        try:
            results = response.get("data", {}).get("actor", {}).get("account", {}).get("alerts", {}).get("violations",
                                                                                                         {}).get(
                "results", [])
            alerts = results
        except (KeyError, AttributeError):
            pass

        return alerts

    @action(description="NEWRELIC: Get API request limit and usage information.")
    def get_api_limits(self) -> Dict:
        """
        Get information about API rate limits and current usage

        Returns:
            API limit information
        """
        response = requests.get(
            f"{self.rest_api_base_url}/user/api_keys/limits",
            headers=self.rest_headers
        )

        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to get API limits: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise Exception(error_msg)

    @action(description="NEWRELIC: Get error traces for an application.")
    def get_error_traces(
            self,
            app_name: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            limit: int = 20
    ) -> List[Dict]:
        """
        Get error traces for an application

        Args:
            app_name: Application name
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            limit: Maximum number of error traces to return

        Returns:
            List of error traces
        """
        # Convert datetime objects to milliseconds since epoch if needed
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        elif isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time = int(dt.timestamp() * 1000)

        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        elif isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time = int(dt.timestamp() * 1000)

        # Build NRQL query for error traces
        nrql = (
            f"SELECT * FROM TransactionError WHERE appName = '{app_name}' "
            f"AND timestamp >= {start_time} AND timestamp <= {end_time} "
            f"LIMIT {limit}"
        )

        return [self.query_nrdb(nrql)]

    @action(description="NEWRELIC: Get transaction traces.")
    def get_transaction_traces(
            self,
            app_name: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            transaction_name: str = None,
            min_duration: float = None,
            limit: int = 20
    ) -> Dict:
        """
        Get transaction traces for an application

        Args:
            app_name: Application name
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            transaction_name: Optional transaction name filter
            min_duration: Minimum duration in seconds
            limit: Maximum number of traces to return

        Returns:
            Transaction traces
        """
        # Convert datetime objects to milliseconds since epoch if needed
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        elif isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time = int(dt.timestamp() * 1000)

        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        elif isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time = int(dt.timestamp() * 1000)

        # Build NRQL query for transaction traces
        nrql = f"SELECT * FROM Transaction WHERE appName = '{app_name}' AND timestamp >= {start_time} AND timestamp <= {end_time}"

        if transaction_name:
            nrql += f" AND name = '{transaction_name}'"

        if min_duration:
            # Convert seconds to milliseconds
            min_duration_ms = min_duration * 1000
            nrql += f" AND duration >= {min_duration_ms}"

        nrql += f" ORDER BY duration DESC LIMIT {limit}"

        return self.query_nrdb(nrql)

    @action(description="NEWRELIC: Get infrastructure hosts information.")
    def get_infrastructure_hosts(self) -> List[Dict]:
        """
        Get information about infrastructure hosts

        Returns:
            List of hosts with their details
        """
        nrql = "SELECT latest(hostname) as hostname, latest(coreCount) as cores, latest(memoryTotalBytes) as totalMemory, latest(diskTotalBytes) as totalDisk FROM SystemSample FACET hostname LIMIT 1000"

        response = self.query_nrdb(nrql)

        hosts = []
        try:
            results = response.get("data", {}).get("actor", {}).get("account", {}).get("nrql", {}).get("results", [])
            hosts = results
        except (KeyError, AttributeError):
            pass

        return hosts

    @action(description="NEWRELIC: Get host metrics.")
    def get_host_metrics(
            self,
            hostname: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime]
    ) -> Dict:
        """
        Get system metrics for a specific host

        Args:
            hostname: Host name
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)

        Returns:
            Host metrics
        """
        # Convert datetime objects to milliseconds since epoch if needed
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        elif isinstance(start_time, str):
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            start_time = int(dt.timestamp() * 1000)

        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        elif isinstance(end_time, str):
            dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            end_time = int(dt.timestamp() * 1000)

        # Build NRQL query for host metrics
        nrql = (
            f"SELECT "
            f"average(cpuPercent) as 'CPU %', "
            f"average(memoryUsedPercent) as 'Memory %', "
            f"average(diskUsedPercent) as 'Disk %', "
            f"average(networkReceiveRate) as 'Network In', "
            f"average(networkTransmitRate) as 'Network Out' "
            f"FROM SystemSample "
            f"WHERE hostname = '{hostname}' "
            f"AND timestamp >= {start_time} AND timestamp <= {end_time} "
            f"TIMESERIES"
        )

        return self.query_nrdb(nrql)

    @action(description="NEWRELIC: Get account information.")
    def get_account_info(self) -> Dict:
        """
        Get information about the current New Relic account

        Returns:
            Account information
        """
        graphql_query = """
        query($accountId: Int!) {
            actor {
                account(id: $accountId) {
                    id
                    name
                    licenseKey
                    datacenter
                }
            }
        }
        """

        variables = {
            "accountId": int(self.account_id)
        }

        return self._make_graphql_request(graphql_query, variables)