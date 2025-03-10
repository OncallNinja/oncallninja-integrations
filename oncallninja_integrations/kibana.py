import requests
import json
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import urllib.parse
import base64

from .action_router import action, ActionRouter


class KibanaClient(ActionRouter):
    def __init__(
            self,
            cloud_id: str = None,
            elasticsearch_url: str = None,
            kibana_url: str = None,
            api_key: str = None,
            username: str = None,
            password: str = None
    ):
        """
        Initialize Managed Kibana API interface

        Args:
            cloud_id: Elastic Cloud ID (preferred method for managed Elasticsearch)
            elasticsearch_url: Direct Elasticsearch URL (alternative to cloud_id)
            kibana_url: Direct Kibana URL (alternative to cloud_id)
            api_key: Elastic API key for authentication (preferred over username/password)
            username: Username for basic authentication
            password: Password for basic authentication
        """
        self.headers = {
            "kbn-xsrf": "true",
            "Content-Type": "application/json"
        }

        # Setup authentication
        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"
            self.auth = None

        # Setup endpoint URLs
        if cloud_id:
            # Parse cloud ID to extract Elasticsearch and Kibana URLs
            decoded = base64.b64decode(cloud_id.split(':')[1]).decode('utf-8')
            domain, es_uuid, kb_uuid = decoded.split('$')

            self.elasticsearch_base_url = f"https://{es_uuid}.{domain}"
            self.kibana_base_url = f"https://{kb_uuid}.{domain}"

        super().__init__()

    @action(description="ELASTIC_SEARCH: Make HTTP request.")
    def _make_elasticsearch_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to Elasticsearch API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request payload

        Returns:
            API response as dictionary
        """
        url = f"{self.elasticsearch_base_url}/{endpoint.lstrip('/')}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
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

    @action(description="KIBANA: Make HTTP request.")
    def _make_kibana_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to Kibana API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request payload

        Returns:
            API response as dictionary
        """
        url = f"{self.kibana_base_url}/api/{endpoint.lstrip('/')}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
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

    @action(description="KIBANA: Get saved objects. Supply the type such as dashboard, visualization, search.")
    def get_saved_objects(self, type: str) -> List[Dict]:
        """
        Get saved objects of specified type

        Args:
            type: Object type (dashboard, visualization, search, etc.)

        Returns:
            List of saved objects
        """
        return self._make_kibana_request("GET", f"saved_objects/_find?type={type}")

    @action(description="KIBANA: get all index patterns.")
    def get_index_patterns_kibana(self) -> List[Dict]:
        """
        Get all index patterns

        Returns:
            List of index patterns
        """
        return self._make_kibana_request("GET", "saved_objects/_find?type=index-pattern")

    @action(description="KIBANA: Get space information")
    def get_space_info(self, space_id: str = "default") -> Dict:
        """
        Get information about a Kibana space

        Args:
            space_id: Space identifier

        Returns:
            Space information
        """
        return self._make_kibana_request("GET", f"spaces/space/{space_id}")

    @action(description="ELASTIC_SEARCH: Get logs. Supply a index_pattern, start_time and end_time, and optional filters")
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

        # For logs we query Elasticsearch directly
        return self._make_elasticsearch_request("POST", f"{index_pattern}/_search", payload)

    @action(description="ELASTIC_SEARCH: Get available fields in a elastic search index pattern")
    def get_log_fields(self, index_pattern: str) -> List[str]:
        """
        Get available fields in the log index pattern

        Args:
            index_pattern: Index pattern to get fields from

        Returns:
            List of available fields
        """
        response = self._make_elasticsearch_request("GET", f"{index_pattern}/_mapping")

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
        Recursively extract fields from Elasticsearch mapping properties
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

    @action(description=" ELASTIC_SEARCH:- Get distinct log levels in an index pattern")
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

        response = self._make_elasticsearch_request("POST", f"{index_pattern}/_search", payload)
        return [bucket["key"] for bucket in response.get("aggregations", {}).get("log_levels", {}).get("buckets", [])]

    @action(description="ELASTIC_SEARCH: search logs in a particular index pattern with a keyword and start_time and end_time")
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

        return self._make_elasticsearch_request("POST", f"{index_pattern}/_search", payload)

    @action(description="ELASTIC_SEARCH: check cluster health.")
    def get_cluster_health(self) -> Dict:
        """
        Get Elasticsearch cluster health

        Returns:
            Cluster health information
        """
        return self._make_elasticsearch_request("GET", "_cluster/health")

    @action(description="KIBANA: check kibana status.")
    def get_kibana_status(self) -> Dict:
        """
        Get Kibana status

        Returns:
            Kibana status information
        """
        return self._make_kibana_request("GET", "status")

    @action(description="ELASTIC_SEARCH: Check all available indexes.")
    def get_elasticsearch_indices(self) -> List[str]:
        """
        Get all indices from Elasticsearch.

        Returns:
            List of index names
        """
        response = self._make_elasticsearch_request("GET", "_cat/indices?format=json")
        return [index["index"] for index in response]


# def main():
#
#     client = KibanaClient(
#         elasticsearch_url="https://4fe97598a9ba4cbe95fbc3aadb9eeb6d.us-central1.gcp.cloud.es.io:443",
#         kibana_url="https://test-aayush.kb.us-central1.gcp.cloud.es.io/",
#         username="elastic",
#         password="5l6CvYGDZ3sUjkwdkM0HXv7E"
#     )
#
#     print("=====================================================================")
#     print(f"Get logs: {client.execute_action("get_logs", {"index_pattern": "cloud-run-logs",
#                                                             "start_time": datetime.utcnow() - timedelta(hours=5),
#                                                             "end_time": datetime.utcnow()})}")
#     print("=====================================================================")
#
#     print(f"Search logs: {client.execute_action("search_logs_by_keyword",
#                                                 {"index_pattern": "cloud-run-logs",
#                                                  "keyword": "karan no",
#                                                  "start_time": datetime.utcnow() - timedelta(minutes=20),
#                                                  "end_time": datetime.utcnow(),
#                                                  "exact_match": True
#                                                  })}")
#
#
# if __name__ == "__main__":
#     main()