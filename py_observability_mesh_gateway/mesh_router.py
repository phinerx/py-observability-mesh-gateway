import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Initialize logger for the module
logger = logging.getLogger(__name__)

# --- Data Models (could be in a separate data_models.py) ---
class ObservabilityData:
    """Represents a generic observability data point (metric, log, trace)."""
    def __init__(self, data_type: str, payload: Dict[str, Any], source_service: str, timestamp: Optional[datetime] = None):
        if not isinstance(data_type, str) or not data_type:
            raise ValueError("data_type must be a non-empty string.")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dictionary.")
        if not isinstance(source_service, str) or not source_service:
            raise ValueError("source_service must be a non-empty string.")

        self.data_type: str = data_type # e.g., "metric", "log", "trace"
        self.payload: Dict[str, Any] = payload
        self.source_service: str = source_service
        self.timestamp: datetime = timestamp if timestamp else datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Converts the data object to a serializable dictionary."""
        return {
            "data_type": self.data_type,
            "payload": self.payload,
            "source_service": self.source_service,
            "timestamp": self.timestamp.isoformat()
        }

    def __repr__(self) -> str:
        return f"ObservabilityData(type='{self.data_type}', source='{self.source_service}', ts='{self.timestamp}')"

class ServiceEndpoint:
    """Represents a target service endpoint for data forwarding."""
    def __init__(self, service_name: str, url: str, protocol: str = "http"):
        if not isinstance(service_name, str) or not service_name:
            raise ValueError("service_name must be a non-empty string.")
        if not isinstance(url, str) or not url:
            raise ValueError("url must be a non-empty string.")
        if not isinstance(protocol, str) or not protocol:
            raise ValueError("protocol must be a non-empty string.")

        self.service_name: str = service_name
        self.url: str = url
        self.protocol: str = protocol # e.g., "http", "grpc", "kafka"

    def __repr__(self) -> str:
        return f"ServiceEndpoint(name='{self.service_name}', url='{self.url}', protocol='{self.protocol}')"

# --- Routing Components ---
class RouterPolicy:
    """Defines a set of rules for routing observability data."""
    def __init__(self, name: str, rules: List[Dict[str, Any]]):
        if not isinstance(name, str) or not name:
            raise ValueError("Policy name must be a non-empty string.")
        if not isinstance(rules, list):
            raise ValueError("Rules must be a list.")
        self.name: str = name
        self.rules: List[Dict[str, Any]] = rules

    def evaluate(self, data: ObservabilityData) -> Optional[str]:
        """
        Evaluates the given observability data against the policy's rules.
        Returns the target service name if a rule matches, otherwise None.
        """
        for rule in self.rules:
            # Basic rule matching: can be extended with regex, JQ-like queries, etc.
            match_type = rule.get("match_type")
            match_value = rule.get("match_value")
            target_service = rule.get("target_service")

            if not all([match_type, match_value, target_service]):
                logger.warning(f"Invalid rule in policy '{self.name}': {rule}")
                continue

            # Example rule conditions
            if match_type == "data_type" and data.data_type == match_value:
                return target_service
            if match_type == "source_service" and data.source_service == match_value:
                return target_service
            if match_type == "payload_key_exists" and match_value in data.payload:
                return target_service
            # More sophisticated matching logic would go here (e.g., regex on message, value comparisons)
        return None

    def __repr__(self) -> str:
        return f"RouterPolicy(name='{self.name}', rules_count={len(self.rules)})"

class ServiceRegistry:
    """Manages the registration and discovery of target service endpoints."""
    def __init__(self):
        self._endpoints: Dict[str, ServiceEndpoint] = {}
        self._logger = logging.getLogger(f"{__name__}.ServiceRegistry")

    def register_service(self, endpoint: ServiceEndpoint):
        """Registers a service endpoint."""
        if not isinstance(endpoint, ServiceEndpoint):
            raise TypeError("Expected ServiceEndpoint object.")
        self._endpoints[endpoint.service_name] = endpoint
        self._logger.info(f"Registered service: {endpoint.service_name} at {endpoint.url}")

    def get_endpoint(self, service_name: str) -> Optional[ServiceEndpoint]:
        """Retrieves a service endpoint by name."""
        return self._endpoints.get(service_name)

    def deregister_service(self, service_name: str):
        """Deregisters a service endpoint."""
        if service_name in self._endpoints:
            del self._endpoints[service_name]
            self._logger.info(f"Deregistered service: {service_name}")
        else:
            self._logger.warning(f"Attempted to deregister non-existent service: {service_name}")

    def __repr__(self) -> str:
        return f"ServiceRegistry(registered_services={len(self._endpoints)})"

class MeshRouter:
    """
    Core component for routing observability data within a service mesh.
    It applies routing policies, discovers target services, and forwards data.
    """
    def __init__(self, service_registry: ServiceRegistry, default_policy: RouterPolicy):
        if not isinstance(service_registry, ServiceRegistry):
            raise TypeError("service_registry must be an instance of ServiceRegistry.")
        if not isinstance(default_policy, RouterPolicy):
            raise TypeError("default_policy must be an instance of RouterPolicy.")

        self._registry = service_registry
        self._default_policy = default_policy
        self._custom_policies: Dict[str, RouterPolicy] = {}
        self._logger = logging.getLogger(f"{__name__}.MeshRouter")
        self._forward_tasks: List[asyncio.Task] = [] # To manage ongoing forwarding tasks

    def add_custom_policy(self, policy: RouterPolicy):
        """Adds a custom routing policy. Custom policies are evaluated before the default."""
        if not isinstance(policy, RouterPolicy):
            raise TypeError("Expected RouterPolicy object.")
        self._custom_policies[policy.name] = policy
        self._logger.info(f"Added custom routing policy: {policy.name}")

    async def route_data(self, data: ObservabilityData) -> bool:
        """
        Routes incoming observability data based on defined policies.
        Returns True if the data was successfully routed or scheduled for forwarding, False otherwise.
        """
        self._logger.debug(f"Attempting to route data from '{data.source_service}' (type: {data.data_type})")

        target_service_name: Optional[str] = None

        # Evaluate custom policies first (order of addition might matter, or could be prioritized)
        for policy_name, policy in self._custom_policies.items():
            target_service_name = policy.evaluate(data)
            if target_service_name:
                self._logger.debug(f"Data matched custom policy '{policy_name}', targeting '{target_service_name}'")
                break
        
        # If no custom policy matched, use default policy
        if not target_service_name:
            target_service_name = self._default_policy.evaluate(data)
            if target_service_name:
                self._logger.debug(f"Data matched default policy, targeting '{target_service_name}'")
            else:
                self._logger.warning(
                    f"No routing policy matched for data from '{data.source_service}' "
                    f"(type: {data.data_type}). Data dropped."
                )
                return False

        target_endpoint = self._registry.get_endpoint(target_service_name)
        if not target_endpoint:
            self._logger.error(f"Target service '{target_service_name}' not found in registry. Data dropped.")
            return False

        try:
            # Schedule forwarding as an asyncio task to avoid blocking the router
            task = asyncio.create_task(self._forward_data(data, target_endpoint))
            self._forward_tasks.append(task) # Keep track of tasks if needed for graceful shutdown
            self._logger.info(
                f"Scheduled forwarding of {data.data_type} from '{data.source_service}' "
                f"to '{target_service_name}' ({target_endpoint.url})"
            )
            return True
        except Exception as e:
            self._logger.exception(f"Failed to schedule data forwarding to '{target_service_name}': {e}")
            return False

    async def _forward_data(self, data: ObservabilityData, target_endpoint: ServiceEndpoint):
        """
        Simulates the actual forwarding of data to the target service endpoint.
        In a real system, this would involve network requests (HTTP, gRPC, Kafka, etc.)
        with proper error handling, retries, and circuit breakers.
        """
        try:
            self._logger.debug(f"Initiating data forwarding to {target_endpoint.protocol}://{target_endpoint.url} for {data.data_type}...")
            # Placeholder for actual network I/O, e.g., using aiohttp for HTTP, or specific client
            # Example: await aiohttp.ClientSession().post(target_endpoint.url, json=data.to_dict())
            await asyncio.sleep(0.02) # Simulate network latency and processing
            self._logger.debug(f"Successfully forwarded data to {target_endpoint.service_name} at {target_endpoint.url}")
        except asyncio.CancelledError:
            self._logger.warning(f"Data forwarding to {target_endpoint.service_name} cancelled.")
            raise
        except Exception as e:
            self._logger.error(
                f"Error forwarding {data.data_type} from '{data.source_service}' "
                f"to '{target_endpoint.service_name}' ({target_endpoint.url}): {e}"
            )
            # Potentially push to a dead-letter queue or trigger an alert
            raise # Re-raise to be handled by the caller or task management

    async def shutdown(self):
        """Performs graceful shutdown, waiting for active forwarding tasks."""
        if self._forward_tasks:
            self._logger.info(f"Waiting for {len(self._forward_tasks)} active forwarding tasks to complete...")
            # Wait for all tasks to finish, with a timeout
            await asyncio.gather(*self._forward_tasks, return_exceptions=True)
            self._logger.info("All forwarding tasks completed or timed out during shutdown.")
