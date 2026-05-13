import json
import logging
from typing import Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor

# Configure logging for the module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MeshRouterConfig:
    """
    Configuration object for the MeshRouter, defining routing rules and endpoints.
    """
    def __init__(self, config_data: Dict[str, Any]):
        self.routes = config_data.get('routes', {})
        self.endpoints = config_data.get('endpoints', {})
        self.default_route = config_data.get('default_route', 'drop') # 'drop' or 'forward_to_default_endpoint'
        logger.info(f"MeshRouterConfig initialized with {len(self.routes)} routes and {len(self.endpoints)} endpoints.")

    def get_route_target(self, data_type: str, source_id: str) -> str:
        """
        Determines the target endpoint name based on data type and source ID.
        """
        # Prioritize specific source_id routes
        if source_id in self.routes.get(data_type, {}):
            return self.routes[data_type][source_id]
        # Fallback to general data_type routes
        elif data_type in self.routes.get('default', {}):
             return self.routes['default'][data_type]
        logger.warning(f"No specific route found for data_type='{data_type}', source_id='{source_id}'. Using default policy.")
        return self.default_route

    def get_endpoint_uri(self, endpoint_name: str) -> str:
        """
        Retrieves the URI for a given endpoint name.
        """
        return self.endpoints.get(endpoint_name)

class MeshRouter:
    """
    Core component responsible for routing incoming observability data to appropriate
    downstream endpoints based on configured rules.
    Supports asynchronous routing and pluggable endpoint handlers.
    """
    def __init__(self, config: MeshRouterConfig, max_workers: int = 4):
        self.config = config
        self.endpoint_handlers: Dict[str, Callable[[str, Dict[str, Any]], None]] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"MeshRouter initialized with {max_workers} worker threads.")

    def register_endpoint_handler(self, endpoint_name: str, handler: Callable[[str, Dict[str, Any]], None]):
        """
        Registers a handler function for a specific named endpoint.
        The handler function should accept (endpoint_uri, data_payload) as arguments.
        """
        if not callable(handler):
            raise TypeError("Handler must be a callable function.")
        self.endpoint_handlers[endpoint_name] = handler
        logger.info(f"Registered handler for endpoint: {endpoint_name}")

    def _process_route(self, endpoint_name: str, data_payload: Dict[str, Any]):
        """
        Internal method to execute routing for a single data payload.
        """
        endpoint_uri = self.config.get_endpoint_uri(endpoint_name)
        if not endpoint_uri:
            logger.error(f"Endpoint URI not found for '{endpoint_name}'. Data will be dropped: {data_payload.get('id', 'N/A')}")
            return

        handler = self.endpoint_handlers.get(endpoint_name)
        if handler:
            try:
                handler(endpoint_uri, data_payload)
                logger.debug(f"Successfully routed data to '{endpoint_name}' via '{endpoint_uri}'.")
            except Exception as e:
                logger.exception(f"Error routing data to '{endpoint_name}' (URI: {endpoint_uri}): {e}")
        else:
            logger.error(f"No handler registered for endpoint '{endpoint_name}'. Data dropped.")

    def route_data(self, data_type: str, source_id: str, data_payload: Dict[str, Any]):
        """
        Routes an incoming observability data payload.
        This method dispatches the routing task to a thread pool for non-blocking operation.
        """
        target_endpoint_name = self.config.get_route_target(data_type, source_id)

        if target_endpoint_name == 'drop':
            logger.info(f"Data from '{source_id}' of type '{data_type}' explicitly dropped by configuration.")
            return

        if target_endpoint_name:
            logger.debug(f"Queueing data for routing: Type='{data_type}', Source='{source_id}', Target='{target_endpoint_name}'")
            self.executor.submit(self._process_route, target_endpoint_name, data_payload)
        else:
            logger.warning(f"No valid target endpoint found for data (type={data_type}, source={source_id}). Data dropped.")

    def shutdown(self):
        """
        Shuts down the internal thread pool, ensuring all pending tasks are completed.
        """
        logger.info("Shutting down MeshRouter executor...")
        self.executor.shutdown(wait=True)
        logger.info("MeshRouter executor shut down.")

# Example Usage (for testing/demonstration)
if __name__ == "__main__":
    # Define a sample configuration
    sample_config_data = {
        "routes": {
            "metrics": {
                "sensor-001": "prometheus-exporter",
                "default": {
                    "metrics": "metrics-aggregator",
                    "logs": "log-storage"
                }
            },
            "logs": {
                "server-app-01": "log-storage"
            }
        },
        "endpoints": {
            "prometheus-exporter": "http://localhost:9091/metrics/push",
            "metrics-aggregator": "https://metrics.example.com/ingest",
            "log-storage": "tcp://logstash.example.com:5044",
            "alerting-system": "amqp://rabbitmq.example.com/alerts"
        },
        "default_route": "log-storage" # If no specific route found, send to log-storage
    }

    router_config = MeshRouterConfig(sample_config_data)
    router = MeshRouter(router_config)

    # Define some dummy endpoint handlers
    def prometheus_handler(uri: str, payload: Dict[str, Any]):
        print(f"[{uri}] Pushing Prometheus metric: {json.dumps(payload)}")

    def metrics_aggregator_handler(uri: str, payload: Dict[str, Any]):
        print(f"[{uri}] Sending to metrics aggregator: {json.dumps(payload)}")

    def log_storage_handler(uri: str, payload: Dict[str, Any]):
        print(f"[{uri}] Storing log entry: {json.dumps(payload)}")

    router.register_endpoint_handler("prometheus-exporter", prometheus_handler)
    router.register_endpoint_handler("metrics-aggregator", metrics_aggregator_handler)
    router.register_endpoint_handler("log-storage", log_storage_handler)

    # Simulate incoming data
    router.route_data("metrics", "sensor-001", {"id": "m1", "value": 12.5, "timestamp": 1678886400})
    router.route_data("metrics", "sensor-002", {"id": "m2", "value": 24.1, "timestamp": 1678886401}) # Should go to metrics-aggregator
    router.route_data("logs", "server-app-01", {"id": "l1", "message": "Service started.", "level": "INFO"})
    router.route_data("logs", "unknown-source", {"id": "l2", "message": "Unhandled log.", "level": "DEBUG"}) # Should go to log-storage (default)
    router.route_data("traces", "service-x", {"id": "t1", "span": "process_request"}) # No specific route, no default for traces, will drop
    router.route_data("metrics", "sensor-003", {"id": "m3", "value": 5.0, "timestamp": 1678886402}) # Should go to metrics-aggregator

    # Give some time for async tasks to complete
    import time
    time.sleep(1)

    router.shutdown()