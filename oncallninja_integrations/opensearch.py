import requests
import json
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import urllib.parse
import base64
import boto3
from requests_aws4auth import AWS4Auth

from .action_router import action, ActionRouter


class AWSOpenSearchClient(ActionRouter):
    def __init__(
            self,
            region: str,
            domain_endpoint: str,
            opensearch_endpoint: Optional[str] = None,
            dashboard_endpoint: Optional[str] = None,
            aws_access_key: Optional[str] = None,
            aws_secret_key: Optional[str] = None,
            aws_session_token: Optional[str] = None,
            iam_role_arn: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None
    ):
        """
        Initialize AWS OpenSearch Service interface

        Args:
            region: AWS region where the OpenSearch domain is deployed
            domain_endpoint: AWS OpenSearch domain endpoint
            opensearch_endpoint: Direct OpenSearch URL (optional, will be derived from domain)
            dashboard_endpoint: Direct OpenSearch Dashboards URL (optional, will be derived from domain)
            aws_access_key: AWS access key for IAM authentication
            aws_secret_key: AWS secret key for IAM authentication
            aws_session_token: AWS session token for temporary credentials
            iam_role_arn: IAM role ARN to assume for authentication
            username: Username for basic authentication (if using fine-grained access control)
            password: Password for basic authentication (if using fine-grained access control)
        """
        self.region = region
        self.domain_endpoint = domain_endpoint.rstrip('/')

        # Set up base URLs
        self.opensearch_base_url = opensearch_endpoint or self.domain_endpoint
        self.dashboards_base_url = dashboard_endpoint or f"{self.domain_endpoint}/_dashboards"

        # Set up headers
        self.headers = {
            "Content-Type": "application/json"
        }

        # Set up authentication
        self.auth = None

        # Basic auth (for fine-grained access control)
        if username and password:
            self.auth = (username, password)

        # IAM auth
        elif aws_access_key and aws_secret_key:
            # If using IAM role, assume the role
            if iam_role_arn:
                sts_client = boto3.client(
                    'sts',
                    region_name=region,
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    aws_session_token=aws_session_token
                )

                assumed_role = sts_client.assume_role(
                    RoleArn=iam_role_arn,
                    RoleSessionName="OpenSearchSession"
                )

                credentials = assumed_role['Credentials']
                aws_access_key = credentials['AccessKeyId']
                aws_secret_key = credentials['SecretAccessKey']
                aws_session_token = credentials['SessionToken']

            # Create AWS4Auth for request signing
            self.aws_auth = AWS4Auth(
                aws_access_key,
                aws_secret_key,
                region,
                'es',  # OpenSearch still uses 'es' service name for signing
                session_token=aws_session_token
            )

        super().__init__()

    @action(description="OPENSEARCH: Make HTTP request.")
    def _make_opensearch_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to OpenSearch API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request payload

        Returns:
            API response as dictionary
        """
        url = f"{self.opensearch_base_url}/{endpoint.lstrip('/')}"

        # Determine which auth to use
        request_auth = self.aws_auth if hasattr(self, 'aws_auth') else self.auth

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                auth=request_auth
            )
            response.raise_for_status()
            return response.json() if response.text else {}
        except requests.exceptions.RequestException as e:
            # Include response text in error if available
            error_msg = f"API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise Exception(error_msg)

    @action(description="DASHBOARDS: Make HTTP request.")
    def _make_dashboards_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to OpenSearch Dashboards API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request payload

        Returns:
            API response as dictionary
        """
        url = f"{self.dashboards_base_url}/api/{endpoint.lstrip('/')}"

        # For dashboard API requests, add XSRF header
        headers = self.headers.copy()
        headers["osd-xsrf"] = "true"  # OpenSearch Dashboards uses osd-xsrf instead of kbn-xsrf

        # Determine which auth to use
        request_auth = self.aws_auth if hasattr(self, 'aws_auth') else self.auth

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                auth=request_auth
            )
            response.raise_for_status()
            return response.json() if response.text else {}
        except requests.exceptions.RequestException as e:
            # Include response text in error if available
            error_msg = f"API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise Exception(error_msg)

    @action(description="DASHBOARDS: Get saved objects. Supply the type such as dashboard, visualization, search.")
    def get_saved_objects(self, type: str) -> List[Dict]:
        """
        Get saved objects of specified type

        Args:
            type: Object type (dashboard, visualization, search, etc.)

        Returns:
            List of saved objects
        """
        return self._make_dashboards_request("GET", f"saved_objects/_find?type={type}")

    @action(description="DASHBOARDS: get all index patterns.")
    def get_index_patterns(self) -> List[Dict]:
        """
        Get all index patterns

        Returns:
            List of index patterns
        """
        return self._make_dashboards_request("GET", "saved_objects/_find?type=index-pattern")

    @action(description="DASHBOARDS: Get space information")
    def get_space_info(self, space_id: str = "default") -> Dict:
        """
        Get information about an OpenSearch Dashboards space

        Args:
            space_id: Space identifier

        Returns:
            Space information
        """
        return self._make_dashboards_request("GET", f"spaces/space/{space_id}")

    @action(description="OPENSEARCH: Get logs. Supply a index_pattern, start_time and end_time, and optional filters")
    def get_logs(
            self,
            index_pattern: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            filters: Optional[Dict] = None,
            size: int = 100,
            sort_field: str = "@timestamp",
            sort_order: str = "desc"
    ) -> Dict:
        """
        Get logs with various filtering options

        Args:
            index_pattern: Index pattern to search
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            filters: Dictionary of filters to apply
            size: Number of results to return
            sort_field: Field to sort by
            sort_order: Sort order ('asc' or 'desc')

        Returns:
            Dictionary containing matching logs
        """
        # Convert datetime objects to ISO format if needed
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()

        # Build the query
        query = {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_time,
                                "lte": end_time
                            }
                        }
                    }
                ]
            }
        }

        # Add custom filters if provided
        if filters:
            for field, value in filters.items():
                if isinstance(value, dict) and ("gte" in value or "lte" in value or "gt" in value or "lt" in value):
                    # Range filter
                    query["bool"]["must"].append({
                        "range": {
                            field: value
                        }
                    })
                elif isinstance(value, list):
                    # Terms filter
                    query["bool"]["must"].append({
                        "terms": {
                            field: value
                        }
                    })
                else:
                    # Match filter
                    query["bool"]["must"].append({
                        "match": {
                            field: value
                        }
                    })

        payload = {
            "query": query,
            "size": size,
            "sort": [
                {
                    sort_field: {
                        "order": sort_order
                    }
                }
            ]
        }

        # For logs we query OpenSearch directly
        return self._make_opensearch_request("POST", f"{index_pattern}/_search", payload)

    @action(description="OPENSEARCH: Get available fields in an index pattern")
    def get_log_fields(self, index_pattern: str) -> List[str]:
        """
        Get available fields in the log index pattern

        Args:
            index_pattern: Index pattern to get fields from

        Returns:
            List of available fields
        """
        response = self._make_opensearch_request("GET", f"{index_pattern}/_mapping")

        fields = []
        # Extract fields from mapping
        for index, mapping in response.items():
            properties = mapping.get("mappings", {}).get("properties", {})
            for field, _ in self._extract_fields_from_properties(properties):
                if field not in fields:
                    fields.append(field)

        return sorted(fields)

    def _extract_fields_from_properties(self, properties, parent=""):
        """
        Recursively extract fields from OpenSearch mapping properties
        """
        fields = []
        for field_name, field_properties in properties.items():
            full_name = f"{parent}{field_name}" if parent else field_name
            fields.append((full_name, field_properties.get("type")))

            if "properties" in field_properties:
                nested_fields = self._extract_fields_from_properties(
                    field_properties["properties"],
                    f"{full_name}."
                )
                fields.extend(nested_fields)

        return fields

    @action(description="OPENSEARCH: Get distinct log levels in an index pattern")
    def get_log_levels(self, index_pattern: str, field: str = "log.level") -> List[str]:
        """
        Get distinct log levels from the index

        Args:
            index_pattern: Index pattern to search
            field: Field containing log level

        Returns:
            List of distinct log levels
        """
        payload = {
            "size": 0,
            "aggs": {
                "log_levels": {
                    "terms": {
                        "field": field,
                        "size": 20
                    }
                }
            }
        }

        response = self._make_opensearch_request("POST", f"{index_pattern}/_search", payload)
        return [bucket["key"] for bucket in response.get("aggregations", {}).get("log_levels", {}).get("buckets", [])]

    @action(
        description="OPENSEARCH: search logs in a particular index pattern with a keyword and start_time and end_time")
    def search_logs_by_keyword(
            self,
            index_pattern: str,
            keyword: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            size: int = 100,
            exact_match: bool = False
    ) -> Dict:
        """
        Search logs by keyword within a specified time range.

        Args:
            index_pattern: Index pattern to search
            keyword: Keyword to search for
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            size: Number of results to return
            exact_match: If True, perform an exact match search across all fields

        Returns:
            Dictionary containing matching logs
        """
        # Convert datetime objects to ISO format if needed
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()

        # Build query
        must_clauses = [
            {
                "range": {
                    "@timestamp": {
                        "gte": start_time,
                        "lte": end_time
                    }
                }
            }
        ]

        if exact_match:
            # Perform exact match search using match_phrase
            must_clauses.append({
                "multi_match": {
                    "query": keyword,
                    "type": "phrase",  # Exact phrase match
                    "fields": ["*"]  # Search in all fields
                }
            })
        else:
            # Perform full-text search
            must_clauses.append({
                "query_string": {
                    "query": keyword
                }
            })

        payload = {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            },
            "size": size,
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }

        return self._make_opensearch_request("POST", f"{index_pattern}/_search", payload)

    @action(description="OPENSEARCH: check cluster health.")
    def get_cluster_health(self) -> Dict:
        """
        Get OpenSearch cluster health

        Returns:
            Cluster health information
        """
        return self._make_opensearch_request("GET", "_cluster/health")

    @action(description="DASHBOARDS: check dashboards status.")
    def get_dashboards_status(self) -> Dict:
        """
        Get OpenSearch Dashboards status

        Returns:
            Dashboards status information
        """
        return self._make_dashboards_request("GET", "status")

    @action(description="OPENSEARCH: Check all available indexes.")
    def get_indices(self) -> List[str]:
        """
        Get all indices from OpenSearch.

        Returns:
            List of index names
        """
        response = self._make_opensearch_request("GET", "_cat/indices?format=json")
        return [index["index"] for index in response]