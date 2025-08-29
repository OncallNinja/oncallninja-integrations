import re
import urllib
from functools import lru_cache
import json # Added for pretty printing the ES query

import requests
from typing import List, Dict, Optional, Union, Any
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from . import util
from .action_router import ActionRouter, action
import logging

class KibanaConfig(BaseModel):
    base_url: str
    username: str
    password: str

class FetchSummaryResponse(BaseModel): # Renamed from FetchSummary to FetchSummaryResponse for consistency
    field_value_map: Dict[str, Dict[str, int]]
    histogram: Dict[datetime, int]

class KibanaClient(ActionRouter):
    def __init__(self, kibana_regional_config: Dict[str, KibanaConfig], max_allowed_hits = 1000):
        """
        Initialize the Kibana client with authentication credentials.
        """
        self.max_allowed_hits = max_allowed_hits
        self.config = kibana_regional_config
        self.logger = logging.getLogger(__name__)

        super().__init__()

    
    @action(description="KIBANA API: Make HTTP request.")
    def _make_request(self, method: str, path: str, params: Optional[Dict] = None, 
                     data: Optional[Dict] = None, region: str = "US") -> Dict:
        """
        Internal method to make authenticated requests to Kibana API.
        """
        kibana_config = None
        if region in self.config:
            kibana_config = self.config[region]
            self.logger.info(f"Using Kibana regional config for: {region}")
        elif "US" in self.config:  # Fallback to US regional if requested region not found
            kibana_config = self.config["US"]
            self.logger.info(
                f"Kibana region '{region}' not found in regional_configs, falling back to US regional config.")
        else:
            raise ValueError(f"No kibana configurations available for region {region}")


        self.logger.info("Making HTTP request")
        url = f"{kibana_config.base_url.rstrip('/')}{path}"
        self.auth = (kibana_config.username, kibana_config.password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json'
        })
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
            error_detail = str(e)
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                try:
                    error_detail += f" | Response: {e.response.text}"
                except Exception: # pylint: disable=broad-except
                    # Ignore if response text is not available for some reason
                    pass
            self.logger.error(f"HTTP request failed: {error_detail}", exc_info=True)
            raise Exception(f"Request failed: {error_detail}")
    
    @action(description="KIBANA API: Get index patterns.")
    @lru_cache
    def get_index_patterns(self, region = "US") -> List[Dict]:
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
        result = self._make_request('GET', path, params=params, region=region)
        return result.get('saved_objects', [])

    @action(description="KIBANA API: Get logs. Supply an index pattern, optionally start and end time, optional log_level, optional search query, optional match_phrase, and size (default set as 100). Maximum time window is 7 day.")
    def get_logs(
        self,
        index_pattern: str,
        start_time: Optional[Union[str, datetime]],
        end_time: Optional[Union[str, datetime]],
        field_filters: Optional[Dict[str, str]],
        log_level: Optional[str] = None,
        search_query: Optional[str] = None,
        match_phrase: Optional[Dict[str, str]] = None,
        region = "US",
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

        if match_phrase:
            for field, phrase in match_phrase.items():
                must_conditions.append({
                    "match_phrase": {
                        field: phrase
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
        response = self._make_request('POST', path, data=count_query, region=region)
        log_count = response.get('count', 0)
        if log_count > self.max_allowed_hits and size > self.max_allowed_hits:
            raise Exception(f"Query would return too many logs ({log_count}). Maximum allowed is {self.max_allowed_hits}. Please refine your query.")

        if log_count == 0:
            raise Exception(f"Query produced 0 logs. Please refine your query.")

        path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
        return self._make_request('POST', path, data=query, region=region)

    @action(description="Fetch logs using a KQL query")
    def fetch_logs_by_kql(self, index_pattern, kql_query, start_time: Optional[Union[str, datetime]],
                          end_time: Optional[Union[str, datetime]], aggregations: Dict, match_phrase: Optional[Dict[str, str]] = None, fields: Optional[List[str]] = None, region="US", size = 100):
        """
        Fetch logs using elasticsearch-py queries.
        """
        kql_query = self._extract_kql_query(kql_query)

        must_conditions = [{"query_string": {"query": kql_query, "analyze_wildcard": True}}]
        time_range = util.convert_to_iso_range(start_time, end_time)
        # Add time range if provided
        if time_range:
            must_conditions.append({"range": {"@timestamp": time_range}})

        if match_phrase:
            for field, phrase in match_phrase.items():
                must_conditions.append({
                    "match_phrase": {
                        field: phrase
                    }
                })

        

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
        if fields:
            query["_source"] = fields

        if aggregations:
            query["aggs"] = aggregations

        # URL encode the index pattern
        encoded_index_pattern = urllib.parse.quote(index_pattern, safe='')

        # First check count
        count_path = f"/api/console/proxy?path={encoded_index_pattern}/_count&method=GET"
        count_result = self._make_request('POST', count_path, data={"query": query["query"]}, region=region)
        log_count = count_result.get("count", 0)
        if log_count > self.max_allowed_hits and size > self.max_allowed_hits:
            raise Exception(
                f"Query would return too many logs ({log_count}). Maximum allowed is {self.max_allowed_hits}. Please refine your query.")
        if log_count == 0:
            raise Exception(f"Query produced 0 logs. Please refine your query.")

        # Now get full results
        path = f"/api/console/proxy?path={encoded_index_pattern}/_search&method=GET"
        return self._make_request('POST', path, data=query, region=region)

    @action(description="KIBANA API: Validate query. Supply a kql query, optional match_phrase, and returns if the query is valid, if not, also returns the error")
    def validate_query(self, kql: str, match_phrase: Optional[Dict[str, str]] = None, region="US") -> (bool, Optional[dict]):
        """Validate KQL using Kibana's API"""
        try:
            # Build must conditions for the query
            must_conditions = []
            
            if kql:
                must_conditions.append({
                    "query_string": {
                        "query": kql,
                        "analyze_wildcard": True
                    }
                })
            
            if match_phrase:
                for field, phrase in match_phrase.items():
                    must_conditions.append({
                        "match_phrase": {
                            field: phrase
                        }
                    })
            
            if must_conditions:
                test_query = {
                    "query": {
                        "bool": {
                            "must": must_conditions
                        }
                    }
                }
            else:
                # If no conditions, use match_all
                test_query = {
                    "query": {
                        "match_all": {}
                    }
                }
                
            # Use Kibana's _validate endpoint
            response = self._make_request(
                'POST',
                '/api/console/proxy?path=_validate/query&method=GET',
                data=test_query,
                region=region
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
            query: Optional[str] = None,
            match_phrase: Optional[Dict[str, str]] = None,
            region = "US"
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

        if match_phrase:
            for field, phrase in match_phrase.items():
                must_conditions.append({
                    "match_phrase": {
                        field: phrase
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
        response = self._make_request('POST', path, data=count_query, region=region)
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

    @lru_cache
    def _fetch_field_details(self, index_pattern: str, region: str = "US") -> List[Dict]:
        """
        Internal helper to fetch detailed field information for an index pattern.
        Returns a list of field objects from Kibana API.
        """
        self.logger.info(f"Fetching field details for index pattern '{index_pattern}' in region '{region}'")
        try:
            response = self._make_request(
                'GET',
                f'/api/index_patterns/_fields_for_wildcard',
                params={
                    "pattern": index_pattern,
                    "meta_fields": ["_source", "_id", "_index", "_score"], # These are standard meta_fields
                },
                region=region
            )
            return response.get('fields', []) # The API returns field details under the 'fields' key
        except Exception as e:
            self.logger.error(f"Failed to fetch field details for index pattern '{index_pattern}': {e}", exc_info=True)
            return []

    @action(description="Fetch all available queryable field names for the given index pattern.")
    @lru_cache # This cache will be on the names, the underlying _fetch_field_details is also cached.
    def get_available_fields(self, index_pattern: str, region = "US") -> set:
        """Get field names using Kibana index patterns API. Returns a set of field names."""
        field_details = self._fetch_field_details(index_pattern, region=region)
        return {field['name'] for field in field_details if 'name' in field}

    @action(description="Fetch fields from a sample log within a given time range.")
    def get_available_fields_from_sample(
        self,
        index_pattern: str,
        region: str = "US",
        size: int = 1,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None
    ) -> set:
        """
        Get fields by sampling documents from the index within a specified time range.
        If start_time and end_time are None, defaults to the last 1 hour.
        """
        self.logger.info(f"Getting available fields from sample for index '{index_pattern}' in region '{region}' between {start_time} and {end_time}")
        try:
            # Determine time range for sampling
            if start_time is None and end_time is None:
                # Default to last 1 hour if no specific range is given for sampling
                self.logger.info(f"Defaulting to last 1 day for field sampling on '{index_pattern}' as no time range was provided.")
                sample_start_time = datetime.utcnow() - timedelta(days=1)
                sample_end_time = datetime.utcnow()
            else:
                sample_start_time = start_time
                sample_end_time = end_time

            iso_time_range = util.convert_to_iso_range(sample_start_time, sample_end_time)

            query_conditions = []
            if iso_time_range: # iso_time_range will be None if both sample_start_time and sample_end_time are None
                 query_conditions.append({"range": {"@timestamp": iso_time_range}})

            # If no time range is effectively set (e.g. both start/end are None and util.convert_to_iso_range returns None),
            # we might want to match all, or ensure a default. The current util.convert_to_iso_range
            # likely provides a default if one is None, so iso_time_range should usually be populated.
            # If query_conditions is empty, it implies match_all within the bool query.

            es_query: Dict[str, Any] = {
                "size": size
            }
            if query_conditions: # Only add query part if there are conditions
                es_query["query"] = {"bool": {"must": query_conditions}}
            else: # Fallback to match_all if no conditions (e.g. time range was invalid or not provided)
                 self.logger.warning(f"No valid time range for field sampling on {index_pattern}, will attempt match_all for sampling query.")
                 es_query["query"] = {"match_all": {}}


            path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
            response = self._make_request('POST', path, data=es_query, region=region)

            fields = set()
            hits_data = response.get('hits', {}).get('hits', [])

            if not hits_data:
                self.logger.warning(
                    f"No documents found in index pattern '{index_pattern}' for the time range "
                    f"'{sample_start_time}' to '{sample_end_time}' when sampling for fields. Returning empty field set."
                )
                return set()

            # Process hits
            for hit in hits_data:
                # Add metadata fields
                    for meta_field in ["_id", "_index", "_score"]:
                        if meta_field in hit:
                            fields.add(meta_field)

                    # Add source fields recursively
                    if '_source' in hit:
                        # Do not add "_source" itself as a field to aggregate on.
                        # Instead, extract its sub-fields.
                        source_fields = self._extract_fields_from_doc(hit['_source'], parent_path="", region=region)
                        fields.update(source_fields)

            if not fields:
                self.logger.warning(f"No fields extracted from sample documents for '{index_pattern}' in time range {sample_start_time} to {sample_end_time}.")
                return set()

            # The region prefix stripping logic:
            # The current _extract_fields_from_doc does not add region prefixes.
            # This processing step might be intended for fields that *could* have a region prefix from other sources
            # or if the _source itself contains keys like "US.fieldname".
            # For now, keeping it to match original behavior observed in the file.
            processed_fields = set()
            for field_name in list(fields):
                if field_name.startswith(f"{region}."):
                    processed_fields.add(field_name[len(f"{region}."):])
                else:
                    processed_fields.add(field_name)

            self.logger.info(f"Found {len(processed_fields)} fields from sample for {index_pattern}: {processed_fields}")
            return processed_fields
        except Exception as e:
            self.logger.error(f"Field fetch from sample failed for index '{index_pattern}': {str(e)}")
            return set()

    def _extract_fields_from_doc(self, doc, parent_path="", region="US"):
        """Extract field names recursively from a document"""
        fields = set()

        if isinstance(doc, dict):
            for key, value in doc.items():
                full_path = f"{parent_path}.{key}" if parent_path else key
                fields.add(full_path)

                # Recurse into nested objects
                if isinstance(value, (dict, list)):
                    nested_fields = self._extract_fields_from_doc(value, full_path, region)
                    fields.update(nested_fields)

        elif isinstance(doc, list) and doc and isinstance(doc[0], dict):
            # For arrays of objects, process the first element
            nested_fields = self._extract_fields_from_doc(doc[0], parent_path, region)
            fields.update(nested_fields)

        return fields

    def _get_nested_value(self, data_dict: Dict, path: str) -> Any:
        """
        Retrieves a value from a nested dictionary using a dot-separated path.
        Example: _get_nested_value({"a": {"b": 1}}, "a.b") == 1
        Handles paths that may include integer indices for lists (e.g., "array.0.field").
        """
        keys = path.split('.')
        value = data_dict
        for key in keys:
            if isinstance(value, dict):
                if key in value:
                    value = value[key]
                else:
                    self.logger.debug(f"Key '{key}' not found in dict for path '{path}' in _get_nested_value")
                    return None
            elif isinstance(value, list):
                try:
                    idx = int(key)
                    if 0 <= idx < len(value):
                        value = value[idx]
                    else:
                        self.logger.debug(f"Index {idx} out of bounds for list (len {len(value)}) for path '{path}' in _get_nested_value")
                        return None  # Index out of bounds
                except ValueError:
                    self.logger.debug(f"Non-integer index '{key}' for list in path '{path}' in _get_nested_value")
                    return None  # Key is not a valid integer index for a list
            else:
                # Value is not a dict or list, so cannot go deeper
                self.logger.debug(f"Cannot resolve path '{path}' at key '{key}'. Current value type: {type(value)}, not a dict or list.")
                return None
        return value

    @action(description="Fetch all available unique values for a specified field, or for multiple fields, based on an index pattern and time range.")
    def fetch_available_field_values(
        self,
        index_pattern: str,
        start_time: Optional[Union[str, datetime]],
        end_time: Optional[Union[str, datetime]],
        target_field: Optional[str] = None,
        region: str = "US",
        max_values_per_field: int = 25,
        max_fields_to_aggregate: int = 20
    ) -> Dict[str, List[str]]:
        """
        Fetches all unique values for a given field (or fields) within a specified time period.

        Args:
            index_pattern: The index pattern to search against.
            start_time: The start of the time range.
            end_time: The end of the time range.
            target_field: Optional. If specified, fetches values only for this field.
                          Otherwise, fetches for multiple fields (up to max_fields_to_aggregate).
            region: The Kibana region to query.
            max_values_per_field: The maximum number of unique values to return per field.
            max_fields_to_aggregate: If target_field is None, the max number of fields to aggregate on.

        Returns:
            A dictionary where keys are field names and values are lists of unique strings for that field.
        """
        self.logger.info(
            f"Fetching available field values for index_pattern='{index_pattern}', "
            f"target_field='{target_field}', region='{region}'"
        )

        time_range = util.convert_to_iso_range(start_time, end_time)

        fields_to_process = []
        if target_field:
            fields_to_process.append(target_field)
            # Note: If target_field is specified, we currently trust it.
            # A future enhancement could validate it against _fetch_field_details
            # and try to use its .keyword version if it's text and not aggregatable.
        else:
            try:
                self.logger.info(
                    f"Fetching field details to determine aggregatable fields for index_pattern='{index_pattern}', region='{region}'"
                )
                all_field_details = self._fetch_field_details(index_pattern, region=region)
                
                aggregatable_field_details = [
                    fd for fd in all_field_details if fd.get('aggregatable') is True and 'name' in fd
                ]
                
                self.logger.info(f"Found {len(aggregatable_field_details)} aggregatable fields for '{index_pattern}': {[fd['name'] for fd in aggregatable_field_details]}")

                print(f"ALL AGGREGATABLE: {aggregatable_field_details}")
                if not aggregatable_field_details:
                     self.logger.warning(f"No aggregatable fields found via metadata for {index_pattern}.")
                     # This will lead to "No fields to process for aggregation" later.

                # Sort by name for deterministic selection if max_fields_to_aggregate is hit
                aggregatable_field_details.sort(key=lambda x: x['name'])

                if len(aggregatable_field_details) > max_fields_to_aggregate:
                    self.logger.warning(
                        f"Found {len(aggregatable_field_details)} aggregatable fields, but will only process the first {max_fields_to_aggregate} "
                        f"due to max_fields_to_aggregate limit."
                    )
                    fields_to_process = [fd['name'] for fd in aggregatable_field_details[:max_fields_to_aggregate]]
                else:
                    fields_to_process = [fd['name'] for fd in aggregatable_field_details]
                
                self.logger.info(f"Fields selected for aggregation based on metadata: {fields_to_process}")

            except Exception as e:
                self.logger.error(f"Exception during field discovery using metadata for {index_pattern}: {e}", exc_info=True)
                if target_field:
                    return {target_field: []}
                return {}

        if not fields_to_process: # This log might be redundant if the one above catches empty available_fields
            self.logger.warning(f"No fields to process for aggregation for index_pattern='{index_pattern}', target_field='{target_field}'. fields_to_process is empty.")
            if target_field:
                return {target_field: []}
            return {}

        # Base query: if time_range is specified, filter by it. Otherwise, match all.
        query_part: Dict[str, Any] = {"match_all": {}}
        if time_range: # util.convert_to_iso_range can return None
            query_part = {"range": {"@timestamp": time_range}}

        es_query: Dict[str, Any] = {
            "query": query_part,
            "size": 0,  # We only care about aggregations
            "aggs": {}
        }

        agg_key_to_original_field_map: Dict[str, str] = {}
        # The fields_to_process now contains names of fields confirmed to be aggregatable
        for i, field_name in enumerate(fields_to_process):
            # No need to append .keyword here, as we've selected aggregatable fields (which would include .keyword versions)
            sanitized_agg_field_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', field_name)
            agg_key = f"values_for_{sanitized_agg_field_name}_{i}"

            es_query["aggs"][agg_key] = {
                "terms": {
                    "field": field_name, # Use the already vetted aggregatable field name
                    "size": max_values_per_field,
                    "order": {"_count": "desc"}
                }
            }
            agg_key_to_original_field_map[agg_key] = field_name # Map back to itself, as it's the correct field name

        results: Dict[str, List[str]] = {}
        if not es_query["aggs"]:
             self.logger.warning("No aggregations to perform (e.g. no fields identified).")
             if target_field: # Should have been caught by 'if not fields_to_process' but as a safeguard
                return {target_field: []}
             return {}

        try:
            encoded_index_pattern = urllib.parse.quote(index_pattern, safe='')
            path = f"/api/console/proxy?path={encoded_index_pattern}/_search&method=GET"
            # Log the query before sending
            try:
                self.logger.info(f"Elasticsearch query for field values on '{index_pattern}': {json.dumps(es_query, indent=2)}")
            except Exception as log_e: # pylint: disable=broad-except
                 self.logger.info(f"Elasticsearch query for field values (raw, json dump failed: {log_e}): {es_query}")

            response = self._make_request('POST', path, data=es_query, region=region)

            if response and 'aggregations' in response:
                for agg_key, agg_data in response['aggregations'].items():
                    original_field_name = agg_key_to_original_field_map.get(agg_key)
                    if original_field_name:
                        # Ensure keys are strings, as they can sometimes be numbers or booleans from ES
                        values = [str(bucket['key']) for bucket in agg_data.get('buckets', [])]
                        results[original_field_name] = values
            else:
                self.logger.warning(f"No aggregations found in response for query on {index_pattern}. Query: {es_query}")

        except Exception as e:
            self.logger.error(f"Error fetching field values for {index_pattern}: {e}")
            if target_field:
                return {target_field: []}
            # Return whatever might have been collected if error occurred mid-processing, or empty
            return results

        return results

    @action(description="Fetch summary. Supply an index pattern, optionally start and end time, optional match_phrase.")
    def fetch_summary(
        self,
        index_pattern: str,
        start_time: Optional[Union[str, datetime]],
        end_time: Optional[Union[str, datetime]],
        sample_size = 500,
        kql_query: Optional[str] = None,
        match_phrase: Optional[Dict[str, str]] = None,
        region: str = "US"
    ) -> FetchSummaryResponse:
        """
        Fetches a summary from a sample of log entries, including counts of unique field values
        and a date histogram.

        Args:
            index_pattern: The index pattern to search against.
            start_time: The start of the time range.
            end_time: The end of the time range.
            region: The Kibana region to query.

        Returns:
            A FetchSummaryResponse object containing a map of field value counts and histogram data.
        """
        self.logger.info(
            f"Fetching summary for index_pattern='{index_pattern}', region='{region}'"
        )
        if sample_size > 1000:
            raise ValueError("Sample size cannot be greater than 1000")

        time_range = util.convert_to_iso_range(start_time, end_time)
        try:
            # Build must conditions for the query
            must_conditions = []
            if kql_query:
                must_conditions.append({
                    "query_string": {
                        "query": kql_query,
                        "analyze_wildcard": True
                    }
                })
            if match_phrase:
                for field, phrase in match_phrase.items():
                    must_conditions.append({
                        "match_phrase": {
                            field: phrase
                        }
                    })

            query_data = {
                "version": True,
                "size": sample_size,
                "sort": [{"@timestamp": {"order": "desc", "unmapped_type": "boolean"}}],
                "_source": {"excludes": []},
                "aggs": {"2": {"date_histogram": {"field": "@timestamp", "calendar_interval": "1h", "time_zone": "America/Los_Angeles", "min_doc_count": 1}}},
                "stored_fields": ["*"],
                "script_fields": {},
                "docvalue_fields": [{"field": "@timestamp", "format": "date_time"}, {"field": "time", "format": "date_time"}],
                "query": {"bool": {"must": must_conditions, "filter": [{"match_all": {}}, {"range": {"@timestamp": time_range}}], "should": [], "must_not": []}},
                "highlight": {"pre_tags": ["@kibana-highlighted-field@"], "post_tags": ["@/kibana-highlighted-field@"], "fields": {"*": {}}}
            }

            encoded_index_pattern = urllib.parse.quote(index_pattern, safe='')
            path = f"/api/console/proxy?path={encoded_index_pattern}/_search&method=GET"
            response = self._make_request('POST', path, data=query_data, region=region)

            current_field_value_map: Dict[str, Dict[str, int]] = {}
            hits_data = response.get('hits', {}).get('hits', [])
            
            for hit in hits_data:
                if '_source' in hit:
                    source_fields = self._extract_fields_from_doc(hit['_source'], parent_path="", region=region)
                    for field in source_fields:
                        value = self._get_nested_value(hit['_source'], field)
                        if value is not None:
                            if field not in current_field_value_map:
                                current_field_value_map[field] = {}

                            value_str = str(value)
                            if len(value_str) > 100:
                                value_str = value_str[100:] + "..."
                            current_field_value_map[field][value_str] = current_field_value_map[field].get(value_str, 0) + 1
            
            histogram_result: Dict[datetime, int] = {}
            aggregations_data = response.get('aggregations')

            if aggregations_data:
                histo_agg_data = aggregations_data.get("2")
                if histo_agg_data and 'buckets' in histo_agg_data:
                    for bucket in histo_agg_data['buckets']:
                        dt_key = datetime.fromtimestamp(bucket['key'] / 1000.0, tz=timezone.utc)
                        histogram_result[dt_key] = bucket['doc_count']
            else:
                self.logger.warning(
                    f"No aggregations found in response for fetch_summary on {index_pattern}."
                )

            return FetchSummaryResponse(
                field_value_map=current_field_value_map,
                histogram=histogram_result
            )

        except Exception as e:
            self.logger.error(f"Error during fetch_summary for {index_pattern}: {str(e)}")
            return FetchSummaryResponse(field_value_map={}, histogram={})

    @action(description="Generates a Kibana Discover URL for the given query, time range, index pattern, and optional match_phrase.")
    def generate_kibana_url(
        self,
        kql_query: str,
        start_time: str,
        end_time: str,
        index_pattern: str,
        match_phrase: Optional[Dict[str, str]] = None,
        region: str = "US"
    ) -> str:
        """
        Generates a Kibana Discover URL for the given query, time range, and index pattern.

        Args:
            kql_query: The KQL query string.
            start_time: The start of the time range (datetime object or ISO string).
            end_time: The end of the time range (datetime object or ISO string).
            index_pattern: The title of the Kibana index pattern (e.g., "api-logs*").
            region: The Kibana region.

        Returns:
            A string representing the Kibana Discover URL.

        Raises:
            ValueError: If the specified region or index pattern title is not found.
        """
        kibana_config = None
        if region in self.config:
            kibana_config = self.config[region]
        elif "US" in self.config:
            kibana_config = self.config["US"]
            self.logger.info(
                f"Kibana region '{region}' not found in regional_configs, falling back to US regional config for URL generation."
            )
        else:
            raise ValueError(f"No Kibana configurations available for region {region} for URL generation.")

        base_url = kibana_config.base_url.rstrip('/')

        dt_start = datetime.fromisoformat(start_time)
        if dt_start.tzinfo is None or dt_start.tzinfo.utcoffset(dt_start) is None:
            dt_start_utc = dt_start.replace(tzinfo=timezone.utc)
        else:
            dt_start_utc = dt_start.astimezone(timezone.utc)
        processed_start_time_str = dt_start_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        dt_end = datetime.fromisoformat(end_time)
        if dt_end.tzinfo is None or dt_end.tzinfo.utcoffset(dt_end) is None:
            dt_end_utc = dt_end.replace(tzinfo=timezone.utc)
        else:
            dt_end_utc = dt_end.astimezone(timezone.utc)
        processed_end_time_str = dt_end_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        all_index_patterns = self.get_index_patterns(region=region)
        index_pattern_id = None
        # The parameter is 'index_pattern', not 'index_pattern_title' as per the user's latest file version
        for pattern_obj in all_index_patterns:
            if pattern_obj.get('attributes', {}).get('title') == index_pattern:
                index_pattern_id = pattern_obj.get('id')
                break
        
        if not index_pattern_id:
            raise ValueError(f"Index pattern title '{index_pattern}' not found in region '{region}'.")

        # Build filters array for match_phrase
        filters_array = []
        if match_phrase:
            for field, phrase in match_phrase.items():
                # Escape RISON special characters in the phrase
                # In RISON, single quotes must be escaped as '!!' and exclamation marks as '()'
                escaped_phrase = phrase.replace("!", "()").replace("'", "!!")
                filter_obj = (
                    f"('$state':(store:appState),"
                    f"meta:(alias:!n,disabled:!f,index:'{index_pattern_id}',key:{field},negate:!f,"
                    f"params:(query:'{escaped_phrase}'),type:phrase),"
                    f"query:(match_phrase:({field}:'{escaped_phrase}')))"
                )
                filters_array.append(filter_obj)
        
        # Combine filters into RISON array format
        if filters_array:
            filters_rison = "!(" + ",".join(filters_array) + ")"
        else:
            filters_rison = "!()"

        # RISON uses single quotes for strings.
        # A single quote ' within a RISON string must be escaped as '!!'.
        # Also, an exclamation mark '!' must be escaped as '!()'.
        # Order of replacement matters: escape '!' first, then \"'.
        escaped_kql_query = kql_query.replace("!", "!()").replace("'", "!!")

        g_state = f"(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{processed_start_time_str}',to:'{processed_end_time_str}'))"
        
        a_state = (
            f"(columns:!()," 
            f"filters:{filters_rison}," 
            f"index:'{index_pattern_id}',"
            f"interval:auto,"
            f"query:(language:kuery,query:'{escaped_kql_query}')," 
            f"sort:!(!('@timestamp',desc)))"
        )
        
        kibana_url = f"{base_url}/app/discover#/?_g={urllib.parse.quote(g_state)}&_a={urllib.parse.quote(a_state)}"
        
        return kibana_url

# if __name__ == "__main__":
#     # Initialize client
#     import os

#     client = KibanaClient(
#         {"US": KibanaConfig(base_url=os.getenv("KIBANA_BASE_URL"),
#                             username=os.getenv("KIBANA_USERNAME"),
#                             password=os.getenv("KIBANA_PASSWORD"))}
#     )
#
#     print(client.execute_action("generate_kibana_url", {"index_pattern": "python-logs*",
#         "kql_query": 'model_id:"67013192-23a4-40b5-aa01-ea4cee786c1a" AND (message:*timeout* OR message:*slow* OR message:*long processing* OR message:*took* OR message:*seconds*)',
#         "start_time": '2025-06-16T14:10:31',
#         "end_time": '2025-06-18T14:13:22.209059'}))
    # print(client.execute_action("fetch_logs_by_kql",
    #                             {"index_pattern": "api-logs*",
    #                              "kql_query": 'level:error AND (msg:"Can\'t import files since model has been deleted" OR msg:"Hello!") AND nanonets_api_server',
    #                              "start_time": datetime(2025, 5, 26, 13, 27, 30, 828019),
    #                              "end_time": datetime(2025, 5, 26, 13, 28, 13, 828024),
    #                              "aggregations": {
    #                                  "error_types": {"terms": {"field": "error_code", "size": 5}},
    #                                  "service_impact": {"terms": {"field": "service.name", "size": 3}}
    #                              }, "region": "EU"}))

    # print(client.execute_action("fetch_logs_by_kql", {
    #     "index_pattern": "python-logs*", "kql_query": "IN.model_id:\"6df0b2ce-af7a-41c8-9363-fca72c6b3108\"",
    #     "start_time": datetime(2025, 6, 6, 00, 00, 00, 828019),
    #     "end_time": datetime(2025, 6, 7, 23, 00, 00, 828019),
    #     "region": "IN"
    # }))

    # print(client.execute_action("fetch_logs_by_kql",
    #                             {"index_pattern": "api-logs*", "kql_query": 'level:error AND (msg:"Can\'t import files since model has been deleted" OR msg:"Hello!") AND nanonets_api_server',
    #                              "start_time": datetime(2025, 5, 26, 13, 27, 30, 828019),
    #                              "end_time": datetime(2025, 5, 26, 13, 28, 13, 828024),
    #                              "aggregations": {
    #                                 "error_types": {"terms": {"field": "error_code", "size": 5}},
    #                                 "service_impact": {"terms": {"field": "service.name", "size": 3}}
    #                             },"region": "EU"}))

    # print(client.execute_action("validate_query", {"kql": 'model_id:"428b544f-018e-4098-bc4d-2218e8241e04" AND message:"*500*"'}))
    # print(client.execute_action("get_index_patterns", {}))
    # print(client.execute_action("get_log_count", {"index_pattern": "python-logs*", "query": 'model_id:"6df0b2ce-af7a-41c8-9363-fca72c6b3108"', "region": "IN", "start_time": datetime(2025, 6, 6, 0, 15, 00, 828019),
    #     "end_time": datetime(2025, 6, 6, 13, 45, 00, 828019)}))
    # print(client.execute_action("get_available_fields", {"index_pattern": "python-logs*", "region": "IN"}))
    # print(client.execute_action("fetch_available_field_values", {"index_pattern": "logstash*", "start_time": datetime(2025, 6, 8, 19, 34, 30, 828019), "end_time": datetime(2025, 6, 10, 13, 28, 13, 828024), "region": "IN"}))
    # print(client.execute_action("fetch_summary", {"index_pattern": "api-logs*", 
    #                                               "start_time": datetime(2025, 8, 28, 0, 00, 0, 828019), 
    #                                               "end_time": datetime(2025, 8, 29, 0, 0, 0, 828024), 
    #                                               "region": "US", 
    #                                               "kql_query": "model_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf AND page_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf",
    #                                               "match_phrase": {'request_id': 'Root=1-68b058cb-4222d4467ce8906f44477b93'}}))
    # print(client.execute_action("fetch_logs_by_kql", {"index_pattern": "api-logs*", 
    #                                               "start_time": datetime(2025, 8, 28, 0, 00, 0, 828019), 
    #                                               "end_time": datetime(2025, 8, 29, 0, 0, 0, 828024), 
    #                                               "region": "US", 
    #                                               "kql_query": "model_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf AND page_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf",
    #                                               "match_phrase": {'request_id': 'Root=1-68b058cb-4222d4467ce8906f44477b93'},
    #                                               "fields": ['@timestamp', 'model_id', 'request_id']}))
    # print(client.execute_action("get_log_count", {"index_pattern": "api-logs*", 
    #                                               "start_time": datetime(2025, 8, 28, 0, 00, 0, 828019), 
    #                                               "end_time": datetime(2025, 8, 29, 0, 0, 0, 828024), 
    #                                               "region": "US", 
    #                                               "query": "model_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf AND page_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf",
    #                                               "match_phrase": {'request_id': 'Root=1-68b058cb-4222d4467ce8906f44477b93'}}))
    # print(client.execute_action("generate_kibana_url", {"index_pattern": "api-logs*", 
    #                                               "start_time": '2025-08-28T00:00:00',
    #                                               "end_time": '2025-08-29T00:00:00',
    #                                               "region": "US", 
    #                                               "kql_query": "model_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf AND page_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf",
    #                                               "match_phrase": {'request_id': 'Root=1-68b058cb-4222d4467ce8906f44477b93'}}))
    # print(client.execute_action("get_available_fields_from_sample", {"index_pattern": "python-logs*", "region": "IN"}))"2025-06-05T19:34:40.188Z","lte":"2025-06-08T19:34:40.188Z"
    # logs = client.execute_action(
    #                            "get_logs",
    #                            {"index_pattern": "api-logs*",
    #                             "search_query": "model_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf AND page_id:12a4b32d-7d99-11f0-9f18-5a6f5dd07fdf",
    #                             "match_phrase": {'request_id': 'Root=1-68b058cb-4222d4467ce8906f44477b93'},
    #                             "start_time": datetime(2025, 8, 28, 0, 00, 0, 828019),
    #                             "end_time": datetime(2025, 8, 29, 0, 0, 0, 828024),
    #                             "fields": ["@timestamp", "error"],
    #                             "size": 50})
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
