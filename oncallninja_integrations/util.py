import logging
from datetime import datetime, timedelta
from typing import Optional, Union

logger = logging.getLogger(__name__)

def convert_to_iso_range(start_time: Optional[Union[str, datetime]],
                          end_time: Optional[Union[str, datetime]], max_window = timedelta(days=7)) -> dict:
    if not start_time and not end_time:
        return {}

    start_dt, end_dt = None, None
    if start_time:
        if isinstance(start_time, str):
            start_dt = datetime.fromisoformat(start_time)
        else:
            start_dt = start_time

    if end_time:
        if isinstance(end_time, str):
            end_dt = datetime.fromisoformat(end_time)
        else:
            end_dt = end_time

    # Calculate time difference
    if start_dt and end_dt:
        time_diff = end_dt - start_dt

        # Adjust time window if it exceeds 7 days
        if max_window and time_diff > max_window:
            logger.warning(
                f"Time window of {time_diff} exceeds maximum allowed {max_window}. "
                f"Adjusting to {max_window} days window starting at {start_dt.isoformat()}"
            )
            end_dt = start_dt + max_window

    # Convert back to ISO format strings for the query
    if not start_dt:
        return {
            "lte": end_dt.isoformat()
        }

    if not end_dt:
        return {
            "gte": start_dt.isoformat(),
        }
    return {
        "gte": start_dt.isoformat(),
        "lte": end_dt.isoformat()
    }