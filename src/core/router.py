import logging
from typing import Dict, Any, List, Callable, Awaitable
from collections import defaultdict
import asyncio

logger = logging.getLogger(__name__)

# Placeholder for actual data models and sink interfaces
class ObservabilityEvent:
    def __init__(self, event_type: str, payload: Dict[str, Any], metadata: Dict[str, Any]):
        self.event_type = event_type  # e.g., "log", "metric", "trace"
        self.payload = payload
        self.metadata = metadata     # e.g., {"source": "host-1", "service": "app-x", "tags": ["prod", "us-east-1"]}

    def get_tag(self, key: str, default=None):
        return self.metadata.get("tags", {}).get(key, default)

    def __repr__(self):
        return f"ObservabilityEvent(type={self.event_type}, metadata={self.metadata})"

class BaseSink:
    async def ingest(self, event: ObservabilityEvent) -> bool:
        raise NotImplementedError

    def get_name(self) -> str:
        raise NotImplementedError

    async def close(self):
        pass # Optional cleanup

class KafkaSink(BaseSink):
    def __init__(self, name: str, topic: str, bootstrap_servers: List[str]):
        self._name = name
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        # self._producer = KafkaProducer(...) # Actual Kafka producer setup
        logger.info(f"Initialized KafkaSink '{name}' for topic '{topic}'")

    async def ingest(self, event: ObservabilityEvent) -> bool:
        logger.debug(f"KafkaSink '{self._name}': Ingesting event {event.event_type} to topic '{self._topic}'")
        # Simulate async send
        await asyncio.sleep(0.01) # Non-blocking I/O
        # producer.send(self._topic, value=json.dumps(event.payload).encode('utf-8'))
        return True

    def get_name(self) -> str:
        return self._name

class RouterConfig:
    def __init__(self, rules: List[Dict[str, Any]], sinks: List[Dict[str, Any]]):
        self.rules = rules
        self.sinks = sinks

class RoutingRule:
    def __init__(self, rule_config: Dict[str, Any]):
        self.name = rule_config.get("name", "unnamed_rule")
        self.match_conditions = rule_config.get("match", {}) # e.g., {"event_type": "log", "metadata.service": "auth-api"}
        self.target_sinks = rule_config.get("sinks", []) # List of sink names

    def matches(self, event: ObservabilityEvent) -> bool:
        for key, expected_value in self.match_conditions.items():
            if key == "event_type":
                if event.event_type != expected_value:
                    return False
            elif key.startswith("metadata."):
                meta_key = key.split("metadata.")[1]
                if event.metadata.get(meta_key) != expected_value:
                    return False
            # Add more complex matching logic here (regex, tag presence, etc.)
        return True

    def __repr__(self):
        return f"RoutingRule(name='{self.name}', match={self.match_conditions}, sinks={self.target_sinks})"

class ObservabilityRouter:
    """
    Core routing component for observability events within the mesh gateway.

    Responsible for evaluating incoming events against a set of defined routing rules
    and dispatching them to the appropriate configured data sinks.
    Supports asynchronous ingestion and dynamic sink registration.
    """
    def __init__(self, config: RouterConfig):
        self._sinks: Dict[str, BaseSink] = {}
        self._rules: List[RoutingRule] = []
        self._initialize_sinks(config.sinks)
        self._initialize_rules(config.rules)
        logger.info(f"ObservabilityRouter initialized with {len(self._rules)} rules and {len(self._sinks)} sinks.")

    def _initialize_sinks(self, sink_configs: List[Dict[str, Any]]):
        """Initializes sink instances from configuration."""
        for sc in sink_configs:
            sink_type = sc.get("type")
            sink_name = sc.get("name")
            if not sink_type or not sink_name:
                logger.error(f"Invalid sink configuration: {sc}. 'type' and 'name' are required.")
                continue

            if sink_type == "kafka":
                self._sinks[sink_name] = KafkaSink(
                    name=sink_name,
                    topic=sc.get("topic"),
                    bootstrap_servers=sc.get("bootstrap_servers", ["localhost:9092"])
                )
            # Add other sink types here (Prometheus, Elasticsearch, custom HTTP, etc.)
            else:
                logger.warning(f"Unsupported sink type '{sink_type}' for sink '{sink_name}'. Skipping.")

    def _initialize_rules(self, rule_configs: List[Dict[str, Any]]):
        """Initializes routing rules from configuration."""
        for rc in rule_configs:
            rule = RoutingRule(rc)
            # Validate target sinks exist
            valid_sinks = []
            for target_sink_name in rule.target_sinks:
                if target_sink_name in self._sinks:
                    valid_sinks.append(target_sink_name)
                else:
                    logger.warning(f"Rule '{rule.name}' targets non-existent sink '{target_sink_name}'. Skipping sink for this rule.")
            rule.target_sinks = valid_sinks # Update rule with only valid sinks
            if valid_sinks:
                self._rules.append(rule)
            else:
                logger.warning(f"Rule '{rule.name}' has no valid target sinks. It will be ignored.")

    async def route_event(self, event: ObservabilityEvent) -> List[str]:
        """
        Routes a single observability event to all matching sinks.

        Args:
            event: The ObservabilityEvent to route.

        Returns:
            A list of names of sinks to which the event was successfully dispatched.
        """
        dispatched_sinks: List[str] = []
        tasks: List[Awaitable[bool]] = []

        for rule in self._rules:
            if rule.matches(event):
                logger.debug(f"Event {event.event_type} matches rule '{rule.name}'. Targets: {rule.target_sinks}")
                for sink_name in rule.target_sinks:
                    sink = self._sinks.get(sink_name)
                    if sink:
                        tasks.append(sink.ingest(event))
                        dispatched_sinks.append(sink_name)
                    else:
                        logger.error(f"Attempted to route to non-existent sink '{sink_name}' for rule '{rule.name}'.")
            else:
                logger.debug(f"Event {event.event_type} does not match rule '{rule.name}'.")

        if not tasks:
            logger.warning(f"Event {event.event_type} did not match any routing rules. It will be dropped.")
            return []

        # Wait for all ingest operations to complete.
        # Consider adding error handling or retry logic here for production systems.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_dispatches = [s for s, r in zip(dispatched_sinks, results) if r is True]

        if not successful_dispatches:
            logger.error(f"Event {event.event_type} failed to dispatch to any target sinks.")

        return list(set(successful_dispatches)) # Return unique sink names

    async def shutdown(self):
        """Performs graceful shutdown of all registered sinks."""
        logger.info("Shutting down ObservabilityRouter and its sinks...")
        shutdown_tasks = [sink.close() for sink in self._sinks.values()]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        logger.info("All sinks have been shut down.")

# Example Usage (for testing/demonstration)
async def main():
    # Simulate configuration from a file or environment variables
    mock_config = RouterConfig(
        sinks=[
            {"name": "kafka_logs", "type": "kafka", "topic": "observability-logs", "bootstrap_servers": ["localhost:9092"]},
            {"name": "kafka_metrics", "type": "kafka", "topic": "observability-metrics", "bootstrap_servers": ["localhost:9092"]}
        ],
        rules=[
            {
                "name": "route_logs_to_kafka",
                "match": {"event_type": "log", "metadata.service": "auth-api"},
                "sinks": ["kafka_logs"]
            },
            {
                "name": "route_all_metrics_to_kafka",
                "match": {"event_type": "metric"},
                "sinks": ["kafka_metrics"]
            },
            {
                "name": "catch_all_critical",
                "match": {"metadata.severity": "critical"},
                "sinks": ["kafka_logs", "kafka_metrics"] # Example of multi-sink routing
            }
        ]
    )

    router = ObservabilityRouter(mock_config)

    # Simulate incoming events
    log_event = ObservabilityEvent(
        event_type="log",
        payload={"message": "User login failed", "level": "ERROR"},
        metadata={"source": "frontend-01", "service": "auth-api", "timestamp": "...", "severity": "critical"}
    )
    metric_event = ObservabilityEvent(
        event_type="metric",
        payload={"name": "cpu_usage", "value": 75.5, "unit": "%"},
        metadata={"source": "backend-02", "service": "data-processor", "timestamp": "..."}
    )
    unmatched_event = ObservabilityEvent(
        event_type="trace",
        payload={"trace_id": "xyz", "span_count": 5},
        metadata={"source": "gateway", "service": "proxy"}
    )

    print(f"\nRouting event: {log_event}")
    dispatched = await router.route_event(log_event)
    print(f"Dispatched to: {dispatched}")

    print(f"\nRouting event: {metric_event}")
    dispatched = await router.route_event(metric_event)
    print(f"Dispatched to: {dispatched}")

    print(f"\nRouting event: {unmatched_event}")
    dispatched = await router.route_event(unmatched_event)
    print(f"Dispatched to: {dispatched}")

    await router.shutdown()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())