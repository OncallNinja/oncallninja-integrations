from typing import Dict, Any, Optional, get_origin
import inspect
import functools

class ActionRouter:
    """Base class for clients that need action routing capabilities."""

    def __init__(self):
        self._actions = {}
        self._register_actions()

    def _register_actions(self):
        """Register all methods decorated with @action."""
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_is_action') and attr._is_action:
                self._actions[attr._action_name] = attr

    def execute_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific action based on the provided name and parameters."""
        try:
            if action not in self._actions:
                return {"status": "error", "message": f"Unknown action: {action}"}

            action_method = self._actions[action]
            required_params = action_method._required_params
            missing_optional_params = [param for param in action_method._optional_params if param not in params]
            for p in missing_optional_params:
                params[p] = None

            # Validate required parameters
            missing_params = [param for param in required_params if param not in params]
            if missing_params:
                return {
                    "status": "error",
                    "message": f"Missing required parameters for {action}: {', '.join(missing_params)}"
                }

            # Execute the action
            result = action_method(**{k: params[k] for k in params if k in action_method._all_params})
            return {"status": "success", "data": result}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def available_actions(self) -> [dict[str, Any]]:
        """List all available actions with their parameters."""
        result = []
        for name, method in self._actions.items():
            params_info = []
            for param in method._all_params:
                if param in method._required_params:
                    params_info.append({"name": param})
                else:
                    params_info.append({"name": param, "optional": True})

            if params_info:
                result.append({name: {"description": method._description, "params": params_info}})
            else:
                result.append({name: {"description": method._description}})

        return result


def action(name: Optional[str] = None, description: str = ""):
    """Decorator to mark methods as actions that can be executed by the router."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Mark as an action
        wrapper._is_action = True
        wrapper._action_name = name or func.__name__
        wrapper._description = description

        # Get parameter information
        sig = inspect.signature(func)
        # Skip 'self' parameter
        wrapper._all_params = [
            param_name for param_name, param in list(sig.parameters.items())[1:]
        ]
        wrapper._required_params = [
            param_name for param_name, param in list(sig.parameters.items())[1:]
            if param.default is inspect.Parameter.empty and get_origin(param.annotation) in {Optional, None}
        ]
        wrapper._optional_params = [
            param_name for param_name, param in list(sig.parameters.items())[1:]
            if get_origin(param.annotation) not in {Optional, None}
        ]

        return wrapper

    return decorator