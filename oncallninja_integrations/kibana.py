import requests
from typing import List, Dict, Optional, Union
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
        super.__init__()
    
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
    

    @action(description="KIBANA API: Get logs. Supply an index pattern, optionally start and end time, optional log_level, optional search query, and size (default set as 100). Maximum time window is 6 hours.")
    def get_logs(
        self,
        index_pattern: str,
        start_time: Union[str, datetime],
        end_time: Union[str, datetime],
        log_level: Optional[str] = None,
        search_query: Optional[str] = None,
        size: int = 100,
        fields: Optional[List[str]] = None
    ) -> Dict:
        """
        Get logs within a specified time range with optional filters.
        If time window exceeds 6 hours, it will be automatically adjusted to 6 hours
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
        max_window = timedelta(hours=6)
        
        # Adjust time window if it exceeds 6 hours
        if time_diff > max_window:
            self.logger.warning(
                f"Time window of {time_diff} exceeds maximum allowed 6 hours. "
                f"Adjusting to 6 hour window ending at {end_dt.isoformat()}"
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
                    "log.level": log_level
                }
            })
            
        if search_query:
            must_conditions.append({
                "match": {
                    "message": search_query
                }
            })
        
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
        
        path = f"/api/console/proxy?path={index_pattern}/_search&method=GET"
        return self._make_request('POST', path, data=query)


# Example usage
# if __name__ == "__main__":
#     # Initialize client
#     client = KibanaClient(
#         base_url="https://logs.nanonets.com",
#         username="oncallninja",
#         password="oncall"
#     )
    
#     # Get all index patterns
#     print("Index Patterns:")
#     index_patterns = client.get_index_patterns()
#     for pattern in index_patterns:
#         print(f"- {pattern['attributes']['title']} (ID: {pattern['id']})")
    
#     # Get error logs from last 7 days
#     print("\nFetching error logs...")
#     from datetime import datetime, timedelta
    
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days=7)
    
#     logs = client.get_logs(
#         # index_pattern="logs-*",
#         index_pattern="api-logs*",
#         start_time=start_time,
#         end_time=end_time,
#         log_level="error",
#         # search_query="error",
#         fields=["@timestamp", "message", "log.level"],
#         size=50
#     )
    
#     print(f"Found {len(logs.get('hits', {}).get('hits', []))} error logs")
#     for idx, hit in enumerate(logs.get('hits', {}).get('hits', [])):
#         print(hit)
#         # break
#         # print(f"{hit['_source']['@timestamp']}: {hit['_source']['message']}")