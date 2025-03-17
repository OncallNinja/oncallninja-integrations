import requests
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import id_token
from requests_aws4auth import AWS4Auth
import re
import json

from .action_router import action, ActionRouter


class AWSOpenSearchClient(ActionRouter):
    def __init__(
            self,
            domain_endpoint: str,
            auth_method: Optional[str] = None,
            aws_access_key: Optional[str] = None,
            aws_secret_key: Optional[str] = None,
            audience: Optional[str] = None,
    ):
        """
        Initialize AWS OpenSearch Service interface

        Args:
            domain_endpoint: AWS OpenSearch domain endpoint
            auth_method: Authentication method ('aws', 'google', or None)
            aws_access_key: AWS access key for AWS authentication
            aws_secret_key: AWS secret key for AWS authentication
            audience: Audience for Google authentication
        """
        self.domain_endpoint = domain_endpoint.rstrip('/')
        self.opensearch_base_url = self.domain_endpoint
        
        # Extract region from domain endpoint for AWS auth
        region_match = re.search(r'\.([^.]+)\.es\.amazonaws\.com', domain_endpoint)
        self.region = region_match.group(1) if region_match else None
        
        # Store auth credentials
        self.auth_method = auth_method
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.audience = audience
        
        # Auto-detect auth method if not specified
        if self.auth_method is None:
            if aws_access_key and aws_secret_key:
                self.auth_method = "aws"
            elif audience:
                self.auth_method = "google"

        super().__init__()

    def _setup_auth(self) -> Tuple[Dict[str, str], Optional[AWS4Auth]]:
        """
        Set up authentication headers and auth object based on the selected method
        
        Returns:
            Tuple of (headers, auth_object)
        """
        base_headers = {"Content-Type": "application/json"}
        
        if self.auth_method == "aws":
            if not all([self.aws_access_key, self.aws_secret_key, self.region]):
                raise ValueError("AWS authentication requires access_key, secret_key, and a valid domain in a region")
            # AWS Signature v4 auth
            auth = AWS4Auth(
                self.aws_access_key,
                self.aws_secret_key,
                self.region,
                'es'
            )
            return base_headers, auth
            
        elif self.auth_method == "google":
            if not self.audience:
                raise ValueError("Google authentication requires an audience")
            # Google ID token
            token = id_token.fetch_id_token(GoogleRequest(), self.audience)
            headers = base_headers.copy()
            headers['Authorization'] = f'Bearer {token}'
            return headers, None
            
        return base_headers, None

    def _find_timestamp_field(self, index_pattern: str) -> str:
        """
        Find the timestamp field in the index mapping
        
        Args:
            index_pattern: Index pattern to check
            
        Returns:
            Name of the timestamp field
        """
        response = self._make_opensearch_request("GET", f"{index_pattern}/_mapping")
        
        # Look for common timestamp field patterns
        timestamp_patterns = [
            "@timestamp",
            "timestamp",
            "time",
            "created_at",
            "date"
        ]
        
        for index, mapping in response.items():
            properties = mapping.get("mappings", {}).get("properties", {})
            
            # First pass: look for exact matches with date type
            for field, field_info in properties.items():
                if field_info.get("type") == "date":
                    if field in timestamp_patterns:
                        return field
            
            # Second pass: any field of type date
            for field, field_info in properties.items():
                if field_info.get("type") == "date":
                    return field
        
        raise ValueError(f"No timestamp field found in index {index_pattern}")

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
        endpoint = endpoint.lstrip('/')
        url = f"{self.opensearch_base_url}/{endpoint}"

        try:
            headers, auth = self._setup_auth()
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                auth=auth,
                json=data
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
        endpoint = endpoint.lstrip('/')
        url = f"{self.dashboards_base_url}/api/{endpoint}"

        try:
            headers, auth = self._setup_auth()
            # For dashboard API requests, add XSRF header
            headers["osd-xsrf"] = "true"  # OpenSearch Dashboards uses osd-xsrf instead of kbn-xsrf
            
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                auth=auth,
                json=data,
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

    @action(description="OPENSEARCH: Get logs with automatic timestamp field detection")
    def get_logs(
            self,
            index_pattern: str,
            start_time: Union[str, datetime],
            end_time: Union[str, datetime],
            filters: Optional[Dict] = None,
            size: int = 100,
            sort_field: Optional[str] = None,
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
            sort_field: Field to sort by (if None, uses detected timestamp field)
            sort_order: Sort order ('asc' or 'desc')

        Returns:
            Dictionary containing matching logs
        """
        # Find the timestamp field
        timestamp_field = self._find_timestamp_field(index_pattern)
        
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
                            timestamp_field: {
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

        # Use detected timestamp field for sorting if not specified
        sort_field = sort_field or timestamp_field

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

    @action(description="OPENSEARCH: search logs in a particular index pattern with a keyword and start_time and end_time")
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
        # Find the timestamp field
        timestamp_field = self._find_timestamp_field(index_pattern)
        
        # Convert datetime objects to ISO format if needed
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()

        # Build query
        must_clauses = [
            {
                "range": {
                    timestamp_field: {
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
                    timestamp_field: {
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

    @action(description="OPENSEARCH: Create or update index mapping")
    def create_index_mapping(self, index: str, mapping: Dict) -> Dict:
        """
        Create or update an index mapping in OpenSearch.

        Args:
            index: Name of the index
            mapping: Mapping configuration

        Returns:
            API response
        """
        # Check if index exists
        try:
            self._make_opensearch_request("HEAD", index)
            # If index exists, update mapping
            return self._make_opensearch_request("PUT", f"{index}/_mapping", mapping)
        except Exception:
            # If index doesn't exist, create it with mapping
            return self._make_opensearch_request("PUT", index, {"mappings": mapping})

    @action(description="OPENSEARCH: Check all available indexes.")
    def get_indices(self) -> List[str]:
        """
        Get all indices from OpenSearch.

        Returns:
            List of index names
        """
        response = self._make_opensearch_request("GET", "_cat/indices?format=json")
        return [index["index"] for index in response]

    @action(description="OPENSEARCH: Write test logs")
    def write_log(
        self,
        index_name: str,
        log_entry: Dict,
        timestamp_field: Optional[str] = None
    ) -> Dict:
        """
        Write a single log entry to OpenSearch
        
        Args:
            index_name: Name of the index to write to
            log_entry: Log entry to write
            timestamp_field: Name of timestamp field (if None, will be auto-detected)
        """
        # Try to get timestamp field from index, or use default
        try:
            if timestamp_field is None:
                timestamp_field = self._find_timestamp_field(index_name)
        except ValueError:
            timestamp_field = "@timestamp"  # fallback to default
        
        # Add timestamp if not present
        if timestamp_field not in log_entry:
            log_entry[timestamp_field] = datetime.now().isoformat()
        
        # Use the index API
        return self._make_opensearch_request(
            "POST",
            f"{index_name}/_doc",
            data=log_entry
        )
