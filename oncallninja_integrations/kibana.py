import os
from functools import lru_cache

import requests
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta
from .action_router import ActionRouter, action
import logging 

class KibanaClient(ActionRouter):
    def __init__(self, base_url: str, username: str, password: str):
        """
        Initialize the Kibana client with authentication credentials.
        
        Args:
            base_url: Base URL of your Kibana instance (e.g., "https://logs.nanonets.com")
            username: Kibana username
            password: Kibana password
        """
        self.base_url = base_url.rstrip('/')
        self.auth = (username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json'
        })
        self.logger = logging.getLogger(__name__)

        super().__init__()

    
    @action(description="KIBANA API: Make HTTP request.")
    def _make_request(self, method: str, path: str, params: Optional[Dict] = None, 
                     data: Optional[Dict] = None) -> Dict:
        """
        Internal method to make authenticated requests to Kibana API.
        """
        self.logger.info("Making HTTP request")
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=data
            )
            response.raise_for_status()
            self.logger.info("HTTP request successful")
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")
    
    @action(description="KIBANA API: Get index patterns.")
    @lru_cache
    def get_index_patterns(self) -> List[Dict]:
        """
        Get all index patterns from Kibana.
        
        Returns:
            List of index patterns with their details
        """
        path = "/api/saved_objects/_find"
        params = {
            'type': 'index-pattern',
            'fields': 'title'
        }
        result = self._make_request('GET', path, params=params)
        return result.get('saved_objects', [])
    

    @action(description="KIBANA API: Get logs. Supply an index pattern, optionally start and end time, optional log_level, optional search query, and size (default set as 100). Maximum time window is 1 day.")
    def get_logs(
        self,
        index_pattern: str,
        start_time: Union[str, datetime],
        end_time: Union[str, datetime],
        log_level: Optional[str] = None,
        search_query: Optional[str] = None,
        size: int = 1000,
        fields: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """
        Get logs within a specified time range with optional filters.
        If time window exceeds 1 day, it will be automatically adjusted to 1 day
        (looking forward from start_time or backward from end_time).
        
        Args:
            index_pattern: The index pattern to search (e.g., "python-logs-*")
            start_time: Start time (ISO format string or datetime object)
            end_time: End time (ISO format string or datetime object)
            log_level: Filter by log level (e.g., "error", "info")
            search_query: Optional text to search in log messages
            size: Maximum number of logs to return
            fields: List of fields to include in response
            
        Returns:
            Dictionary containing the search results
        """
        # Convert to datetime objects if they're strings
        if isinstance(start_time, str):
            start_dt = datetime.fromisoformat(start_time)
        else:
            start_dt = start_time
            
        if isinstance(end_time, str):
            end_dt = datetime.fromisoformat(end_time)
        else:
            end_dt = end_time
        
        # Calculate time difference
        time_diff = end_dt - start_dt
        max_window = timedelta(days=1)
        
        # Adjust time window if it exceeds 1 day
        if time_diff > max_window:
            self.logger.warning(
                f"Time window of {time_diff} exceeds maximum allowed 1 day. "
                f"Adjusting to 1 day window ending at {end_dt.isoformat()}"
            )
            start_dt = end_dt - max_window
        
        # Convert back to ISO format strings for the query
        start_time_iso = start_dt.isoformat()
        end_time_iso = end_dt.isoformat()
        
        # Build the query
        must_conditions = [
            {
                "range": {
                    "@timestamp": {
                        "gte": start_time_iso,
                        "lte": end_time_iso
                    }
                }
            }
        ]
        
        if log_level:
            must_conditions.append({
                "match": {
                    "error.log.level": log_level
                }
            })
            
        if search_query:
            must_conditions.append({
                "match": {
                    "message": search_query
                }
            })

        if not aggregations:
            aggregations = {}
        query = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "aggs": aggregations,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "size": size
        }
        
        if fields:
            query["_source"] = fields
        
        path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
        return self._make_request('POST', path, data=query)

    @action(description="KIBANA API: Validate query. Supply a kql query, and returns if the query is valid, if not, also returns the error")
    def validate_query(self, kql: str) -> (bool, Optional[dict]):
        """Validate KQL using Kibana's API"""
        try:
            test_query = {
                "query": {
                    "query_string": {
                        "query": kql,
                        "analyze_wildcard": True
                    }
                }
            }
            # Use Kibana's _validate endpoint
            response = self._make_request(
                'POST',
                '/api/console/proxy?path=_validate/query&method=GET',
                data=test_query
            )
            return response["valid"], response.get("error")
        except Exception as e:
            return False, {"reason": str(e)}

    @action(description="Fetch count of logs")
    def get_log_count(
            self,
            index_pattern: str,
            query: Optional[str] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
    ) -> int:
        """Get count of logs matching KQL within time range"""
        # Base time range filter
        time_filter = {
            "range": {
                "@timestamp": {
                    "gte": start_time.isoformat() if start_time else "now-15m",
                    "lte": end_time.isoformat() if end_time else "now"
                }
            }
        }

        # Build the complete query
        count_query = {
            "query": {
                "bool": {
                    "must": [time_filter]
                }
            }
        }

        if query:
            # Add KQL as query_string filter
            count_query["query"]["bool"]["must"].append({
                "query_string": {
                    "query": query,
                    "analyze_wildcard": True,
                    "default_field": "*"
                }
            })

        path = f"/api/console/proxy?path=/{index_pattern}/_count&method=GET"
        response = self._make_request('POST', path, data=count_query)
        return response.get('count', 0)

    @action(description="Fetch all available queryable fields for the given index pattern")
    @lru_cache
    def get_available_fields(self, index_pattern: str) -> set:
        """Get fields using legacy Kibana index patterns API"""
        try:
            response = self._make_request(
                'GET',
                f'/api/index_patterns/_fields_for_wildcard',
                params={
                    "pattern": index_pattern,
                    "meta_fields": ["_source", "_id", "_index", "_score"],
                    "type": "index_pattern",
                    "rollup_index": "",
                    "allow_no_index": True
                }
            )
            return {field['name'] for field in response['fields']}
        except Exception as e:
            print(f"Field fetch failed: {str(e)}")
            return set()

    @action(description="Fetch fields from a sample log")
    def get_available_fields_from_sample(self, index_pattern: str, size=1) -> set:
        """Get fields by sampling documents from the index"""
        try:
            # Get a sample document
            response = self.get_logs(index_pattern, start_time=datetime.utcnow() - timedelta(days=1), end_time=datetime.utcnow(), size=1)

            fields = set()

            # Process hits
            if 'hits' in response and 'hits' in response['hits']:
                for hit in response['hits']['hits']:
                    # Add metadata fields
                    for meta_field in ["_id", "_index", "_score"]:
                        if meta_field in hit:
                            fields.add(meta_field)

                    # Add source fields recursively
                    if '_source' in hit:
                        fields.add("_source")
                        source_fields = self._extract_fields_from_doc(hit['_source'])
                        fields.update(source_fields)

            return fields
        except Exception as e:
            print(f"Field fetch failed: {str(e)}")
            return set()

    def _extract_fields_from_doc(self, doc, parent_path=""):
        """Extract field names recursively from a document"""
        fields = set()

        if isinstance(doc, dict):
            for key, value in doc.items():
                full_path = f"{parent_path}.{key}" if parent_path else key
                fields.add(full_path)

                # Recurse into nested objects
                if isinstance(value, (dict, list)):
                    nested_fields = self._extract_fields_from_doc(value, full_path)
                    fields.update(nested_fields)

        elif isinstance(doc, list) and doc and isinstance(doc[0], dict):
            # For arrays of objects, process the first element
            nested_fields = self._extract_fields_from_doc(doc[0], parent_path)
            fields.update(nested_fields)

        return fields

# Example usage
# if __name__ == "__main__":
#     # Initialize client
#     client = KibanaClient(
#         base_url=os.getenv("KIBANA_BASE_URL"),
#         username=os.getenv("KIBANA_USERNAME"),
#         password=os.getenv("KIBANA_PASSWORD")
#     )
#
#     # print(client.execute_action("get_available_fields", {"index_pattern": "api-logs*"}))
#     # print(client.execute_action("get_available_fields_from_sample", {"index_pattern": "api-logs*"}))
#     print(client.execute_action(
#                                "get_log_count",
#                                {"index_pattern": "api-logs*",
#                                 "query": "error_reason.keyword : \"Avanto export fail\" OR error_reason.keyword : \"inference fail\"",
#                                 "start_time": datetime.fromisoformat('2025-04-23T06:04:35.120209'),
#                                 "end_time": datetime.fromisoformat('2025-04-23T06:19:35.120220')}))


# # "https://logs.nanonets.com/api/log_entries/summary"
#
#
#     # Get all index patterns
#     # print("Index Patterns:")
#     # index_patterns = client.get_index_patterns()
#     # for pattern in index_patterns:
#     #     print(f"- {pattern['attributes']['title']} (ID: {pattern['id']})")
#     #
#     # Get error logs from last 7 days
#     # print("\nFetching error logs...")
#     # from datetime import datetime, timedelta
#     #
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days=7)
#
#     count = client.get_log_count(
#         index_pattern="python-logs*",
#         start_time=start_time,
#         end_time=end_time,
#     )
#     print(f"Log count {count}")
    # logs = client.get_logs(
    #     # index_pattern="logs-*",
    #     index_pattern="api-logs*",
    #     start_time=start_time,
    #     end_time=end_time,
    #     # log_level="error",
    #     # search_query="error",
    #     # fields=["@timestamp", "message"],
    #     size=50
    # )

    # print(logs)
    #
    #
    # print(f"Found {len(logs.get('hits', {}).get('hits', []))} error logs")
    # for idx, hit in enumerate(logs.get('hits', {}).get('hits', [])):
    #     print(hit)
        # break
        # print(f"{hit['_source']['@timestamp']}: {hit['_source']['message']}")