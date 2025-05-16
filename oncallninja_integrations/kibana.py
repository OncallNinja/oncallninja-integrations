import os
import re
import urllib
from functools import lru_cache

import requests
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta

from . import util
from .action_router import ActionRouter, action
import logging

class KibanaClient(ActionRouter):
    def __init__(self, base_url: str, username: str, password: str, max_allowed_hits = 1000):
        """
        Initialize the Kibana client with authentication credentials.
        
        Args:
            base_url: Base URL of your Kibana instance (e.g., "https://logs.nanonets.com")
            username: Kibana username
            password: Kibana password
        """
        self.base_url = base_url.rstrip('/')
        self.max_allowed_hits = max_allowed_hits
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
        start_time: Optional[Union[str, datetime]],
        end_time: Optional[Union[str, datetime]],
        field_filters: Optional[Dict[str, str]],
        log_level: Optional[str] = None,
        search_query: Optional[str] = None,
        size: int = 100,
        fields: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """
        Get logs within a specified time range with optional filters.
        If time window exceeds 1 day, it will be automatically adjusted to 1 day
        (looking forward from start_time or backward from end_time).
        """
        # Convert to datetime objects if they're strings
        time_range = util.convert_to_iso_range(start_time, end_time)
        # Build the query
        must_conditions = [
            {
                "range": {
                    "@timestamp": time_range
                }
            }
        ]

        if log_level:
            if not field_filters:
                field_filters = {}
            field_filters['level'] = log_level

        if field_filters:
            for field, value in field_filters.items():
                # Handle multiple values for the same field (OR condition)
                if isinstance(value, list):
                    should_clauses = []
                    for v in value:
                        should_clauses.append({"term": {field: v}})
                    must_conditions.append({"bool": {"should": should_clauses}})
                else:
                    must_conditions.append({"term": {field: value}})
            
        if search_query:
            must_conditions.append({
                "query_string": {
                    "query": search_query,
                    "analyze_wildcard": True,
                    "default_field": "error.exception.message"
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
        count_query = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
        }
        
        if fields:
            query["_source"] = fields

        path = f"/api/console/proxy?path={index_pattern}/_count&method=GET"
        response = self._make_request('POST', path, data=count_query)
        log_count = response.get('count', 0)
        if log_count > self.max_allowed_hits and size > self.max_allowed_hits:
            raise Exception(f"Query would return too many logs ({log_count}). Maximum allowed is {self.max_allowed_hits}. Please refine your query.")

        if log_count == 0:
            raise Exception(f"Query produced 0 logs. Please refine your query.")

        path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
        return self._make_request('POST', path, data=query)

    @action(description="Fetch logs using a KQL query")
    def fetch_logs_by_kql(self, index_pattern, kql_query, start_time: Optional[Union[str, datetime]], end_time: Optional[Union[str, datetime]], aggregations: Dict, size = 100):
        """
        Fetch logs using elasticsearch-py queries.
        """
        kql_query = self._extract_kql_query(kql_query)

        must_conditions = [{"query_string": {"query": kql_query, "analyze_wildcard": True}}]
        time_range = util.convert_to_iso_range(start_time, end_time)
        # Add time range if provided
        if time_range:
            must_conditions.append({"range": {"@timestamp": time_range}})

        # Create the base query with query_string
        query = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "sort": [{"@timestamp": {"order": "desc"}}],
            "size": size
        }

        if aggregations:
            query["aggs"] = aggregations

        # URL encode the index pattern
        encoded_index_pattern = urllib.parse.quote(index_pattern, safe='')

        # First check count
        count_path = f"/api/console/proxy?path={encoded_index_pattern}/_count&method=GET"
        count_result = self._make_request('POST', count_path, data={"query": query["query"]})
        log_count = count_result.get("count", 0)
        if log_count > self.max_allowed_hits and size > self.max_allowed_hits:
            raise Exception(
                f"Query would return too many logs ({log_count}). Maximum allowed is {self.max_allowed_hits}. Please refine your query.")
        if log_count == 0:
            raise Exception(f"Query produced 0 logs. Please refine your query.")

        # Now get full results
        path = f"/api/console/proxy?path={encoded_index_pattern}/_search&method=GET"
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
            start_time: Optional[Union[str, datetime]],
            end_time: Optional[Union[str, datetime]],
            query: Optional[str] = None
    ) -> int:
        """Get count of logs matching KQL within time range"""
        # Base time range filter
        time_range = util.convert_to_iso_range(start_time, end_time)
        must_conditions = []
        if start_time or end_time:
            must_conditions.append({
                "range": {
                    "@timestamp": time_range
                }
            })

        if query:
            must_conditions.append({
                "query_string": {
                    "query": query,
                    "analyze_wildcard": True
                }
            })

        count_query = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
        }

        path = f"/api/console/proxy?path={index_pattern}/_count&method=GET"
        response = self._make_request('POST', path, data=count_query)
        return response.get('count', 0)

    def _extract_kql_query(self, input_text):
        """
        Extract the actual KQL query from various input formats.

        Args:
            input_text (str): Input that might contain KQL in markdown code blocks

        Returns:
            str: The extracted KQL query
        """
        # Check if the input follows the ```kql ... ``` format
        kql_code_block_pattern = r'```kql\s+(.*?)```'
        match = re.search(kql_code_block_pattern, input_text, re.DOTALL)

        if match:
            # Extract the query from the code block
            return match.group(1).strip()
        else:
            # Return the input as is, assuming it's a direct KQL query
            return input_text.strip()

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
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": util.convert_to_iso_range(start_time=datetime.utcnow() - timedelta(hours=1), end_time=datetime.utcnow())
                                }
                            }
                        ]
                    }
                },
                "size": size
            }
            path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
            response = self._make_request('POST', path, data=query)
            # response = self.get_logs(index_pattern, start_time=datetime.utcnow() - timedelta(days=1), end_time=datetime.utcnow(), field_filters=None, size=1)

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

# # Example usage
# if __name__ == "__main__":
#     # Initialize client
#     client = KibanaClient(
#         base_url=os.getenv("KIBANA_BASE_URL"),
#         username=os.getenv("KIBANA_USERNAME"),
#         password=os.getenv("KIBANA_PASSWORD")
#     )

    # print(client.execute_action("fetch_logs_by_kql",
    #                             {"index_pattern": "api-logs*", "kql_query": 'level:error AND (msg:"Can\'t import files since model has been deleted" OR msg:"Hello!") AND nanonets_api_server',
    #                              "start_time": datetime(2025, 4, 26, 13, 27, 30, 828019),
    #                              "end_time": datetime(2025, 4, 26, 13, 28, 13, 828024),
    #                              "aggregations": {
    #                                 "error_types": {"terms": {"field": "error_code", "size": 5}},
    #                                 "service_impact": {"terms": {"field": "service.name", "size": 3}}
    #                             }}))

    # print(client.execute_action("validate_query", {"kql": 'model_id:"428b544f-018e-4098-bc4d-2218e8241e04" AND message:"*500*"'}))
    # print(client.execute_action("get_index_patterns", {}))
    # print(client.execute_action("get_log_count", {"index_pattern": "api-logs*", "query": 'model_id:"c8f3e035-73b4-4393-b8fc-16c8c19f96d1" AND level:error'}))
    # print(client.execute_action("get_available_fields", {"index_pattern": "api-logs*"}))
    # print(client.execute_action("get_available_fields_from_sample", {"index_pattern": "api-logs*"}))
    # logs = client.execute_action(
    #                            "get_logs",
    #                            {"index_pattern": "api-logs*",
    #                             "log_level": "error",
    #                             "start_time": datetime(2025, 4, 26, 1, 28, 43, 828019),
    #                             "end_time": datetime(2025, 4, 26, 15, 33, 13, 828024),
    #                             "fields": ["@timestamp", "error"],
    #                             "size": 5})
    # print(logs)
    #
    # print(f"Found {len(logs.get('data', {}).get('hits', {}).get('hits', []))} error logs")

    # logs = client.execute_action(
    #                            "get_logs_kql",
    #                            {"kql": {
    #                                "query": {
    #                                    "match": {
    #                                        "_index": "api-logs*"
    #                                    }
    #                                },
    #                                "size": 1
    #                            },
    #                             "size": 5})
    # print(logs)
    #
    # print(f"Found {len(logs.get('data', {}).get('hits', {}).get('hits', []))} error logs")


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
#     logs = client.get_logs(
#         # index_pattern="logs-*",
#         index_pattern="api-logs*",
#         start_time=start_time,
#         end_time=end_time,
#         # log_level="error",
#         # search_query="error",
#         # fields=["@timestamp", "message"],
#         size=50
#     )
#
#     # print(logs)
#
#
#     print(f"Found {len(logs.get('hits', {}).get('hits', []))} error logs")
#     for idx, hit in enumerate(logs.get('hits', {}).get('hits', [])):
#         print(hit)
#         # break
#         # print(f"{hit['_source']['@timestamp']}: {hit['_source']['message']}")