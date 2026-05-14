import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Protocol, Tuple

# Configure logging for the processor
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ObservabilityDataProcessor')

# --- Data Model Protocols ---
class Metric(Protocol):
    """Protocol for a single observability metric."""
    name: str
    value: float
    timestamp: float
    tags: Dict[str, str]

class LogEntry(Protocol):
    """Protocol for a single observability log entry."""
    message: str
    level: str
    timestamp: float
    service: str
    context: Dict[str, Any]

class DataSink(Protocol):
    """Protocol for a data sink where processed data is sent."""
    async def send_metrics(self, metrics: List[Metric]) -> None: ...
    async def send_logs(self, logs: List[LogEntry]) -> None: ...
    async def flush(self) -> None: ...

# --- Configuration and Rule Definitions ---
class ProcessingRule:
    """Defines a rule for data processing (e.g., filtering, aggregation)."""
    def __init__(self, rule_type: str, match_criteria: Dict[str, Any], action: Dict[str, Any]):
        self.rule_type = rule_type
        self.match_criteria = match_criteria
        self.action = action

    def applies_to(self, data_point: Dict[str, Any]) -> bool:
        """Checks if the rule applies to the given data point."""
        for key, value in self.match_criteria.items():
            if data_point.get(key) != value:
                return False
        return True

    def apply(self, data_point: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Applies the rule's action to the data point."""
        if self.rule_type == "filter":
            return None  # Filtered out
        elif self.rule_type == "enrich":
            data_point.update(self.action.get("add_tags", {}))
            return data_point
        elif self.rule_type == "aggregate":
            # This would typically involve stateful aggregation, simplified here
            logger.debug(f"Aggregation rule applied (simplified): {data_point}")
            return data_point
        return data_point

class ProcessorConfig:
    """Configuration for the ObservabilityDataProcessor."""
    def __init__(self, rules: List[Dict[str, Any]], sinks: Dict[str, DataSink]):
        self.processing_rules: List[ProcessingRule] = [
            ProcessingRule(r['type'], r.get('match', {}), r.get('action', {})) for r in rules
        ]
        self.data_sinks: Dict[str, DataSink] = sinks
        logger.info(f"Initialized processor with {len(self.processing_rules)} rules and {len(self.data_sinks)} sinks.")

# --- Mock Data Sink for demonstration ---
class ConsoleDataSink(DataSink):
    """A simple data sink that prints data to the console."""
    async def send_metrics(self, metrics: List[Metric]) -> None:
        for metric in metrics:
            logger.info(f"[METRIC] Name: {metric.name}, Value: {metric.value}, Tags: {json.dumps(metric.tags)}")

    async def send_logs(self, logs: List[LogEntry]) -> None:
        for log in logs:
            logger.info(f"[LOG] Level: {log.level}, Service: {log.service}, Message: {log.message[:80]}...")

    async def flush(self) -> None:
        logger.debug("ConsoleDataSink flush operation completed.")


# --- Core Processor Implementation ---
class ObservabilityDataProcessor:
    """
    Handles the ingestion, processing, and routing of observability metrics and logs.
    Applies configured rules for filtering, enrichment, and aggregation before dispatching
    data to various data sinks.
    """
    def __init__(self, config: ProcessorConfig):
        self.config = config
        self._metric_buffer: List[Metric] = []
        self._log_buffer: List[LogEntry] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_interval = 5  # seconds
        self._flush_task: Optional[asyncio.Task] = None
        logger.info("ObservabilityDataProcessor initialized.")

    async def _apply_processing_rules(self, data_point: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Applies all configured processing rules to a single data point."""
        processed_data = data_point
        for rule in self.config.processing_rules:
            if rule.applies_to(processed_data):
                processed_data = rule.apply(processed_data)
                if processed_data is None:  # Data point was filtered out
                    return None
        return processed_data

    async def _route_to_sinks(self, metrics: List[Metric], logs: List[LogEntry]) -> None:
        """Dispatches processed metrics and logs to all configured data sinks."""
        tasks = []
        for sink_name, sink in self.config.data_sinks.items():
            if metrics:
                tasks.append(sink.send_metrics(metrics))
            if logs:
                tasks.append(sink.send_logs(logs))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                if task.exception():
                    logger.error(f"Error sending data to sink: {task.exception()}")
        logger.debug(f"Routed {len(metrics)} metrics and {len(logs)} logs to sinks.")

    async def ingest_metric(self, raw_metric: Dict[str, Any]) -> None:
        """
        Ingests a single raw metric, applies processing rules, and buffers it.
        """
        processed_metric_data = await self._apply_processing_rules(raw_metric)
        if processed_metric_data:
            try:
                # Basic validation and conversion to Metric Protocol
                metric: Metric = type('MetricImpl', (object,), processed_metric_data)()
                async with self._buffer_lock:
                    self._metric_buffer.append(metric)
                logger.debug(f"Ingested and buffered metric: {metric.name}")
            except (KeyError, TypeError) as e:
                logger.warning(f"Failed to convert processed metric to Metric type: {e} - Data: {processed_metric_data}")

    async def ingest_log_entry(self, raw_log: Dict[str, Any]) -> None:
        """
        Ingests a single raw log entry, applies processing rules, and buffers it.
        """
        processed_log_data = await self._apply_processing_rules(raw_log)
        if processed_log_data:
            try:
                # Basic validation and conversion to LogEntry Protocol
                log_entry: LogEntry = type('LogEntryImpl', (object,), processed_log_data)()
                async with self._buffer_lock:
                    self._log_buffer.append(log_entry)
                logger.debug(f"Ingested and buffered log: {log_entry.message[:50]}...")
            except (KeyError, TypeError) as e:
                logger.warning(f"Failed to convert processed log to LogEntry type: {e} - Data: {processed_log_data}")

    async def _flush_buffers(self) -> None:
        """Periodically flushes buffered metrics and logs to data sinks."""
        while True:
            await asyncio.sleep(self._flush_interval)
            metrics_to_flush: List[Metric] = []
            logs_to_flush: List[LogEntry] = []

            async with self._buffer_lock:
                if self._metric_buffer:
                    metrics_to_flush = self._metric_buffer
                    self._metric_buffer = []
                if self._log_buffer:
                    logs_to_flush = self._log_buffer
                    self._log_buffer = []

            if metrics_to_flush or logs_to_flush:
                logger.info(f"Flushing {len(metrics_to_flush)} metrics and {len(logs_to_flush)} logs.")
                await self._route_to_sinks(metrics_to_flush, logs_to_flush)
            else:
                logger.debug("No data to flush.")

            # Ensure sinks are flushed if they have internal buffers
            for sink in self.config.data_sinks.values():
                await sink.flush()

    def start(self) -> None:
        """Starts the background flushing task."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_buffers())
            logger.info("ObservabilityDataProcessor started flushing task.")
        else:
            logger.warning("Flush task is already running.")

    async def stop(self) -> None:
        """Stops the processor and flushes any remaining data."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                logger.info("ObservabilityDataProcessor flush task cancelled.")

        # Final flush before shutdown
        metrics_to_flush: List[Metric] = []
        logs_to_flush: List[LogEntry] = []
        async with self._buffer_lock:
            if self._metric_buffer:
                metrics_to_flush = self._metric_buffer
                self._metric_buffer = []
            if self._log_buffer:
                logs_to_flush = self._log_buffer
                self._log_buffer = []
        if metrics_to_flush or logs_to_flush:
            logger.info("Performing final flush before shutdown.")
            await self._route_to_sinks(metrics_to_flush, logs_to_flush)
        for sink in self.config.data_sinks.values():
            await sink.flush()
        logger.info("ObservabilityDataProcessor stopped.")

# --- Example Usage (for testing/demonstration) ---
async def main():
    console_sink = ConsoleDataSink()
    processor_config = ProcessorConfig(
        rules=[
            {"type": "filter", "match": {"service": "legacy-app"}, "action": {}},
            {"type": "enrich", "match": {"level": "ERROR"}, "action": {"add_tags": {"severity": "critical"}}},
            {"type": "enrich", "match": {"service": "auth-svc"}, "action": {"add_tags": {"department": "security"}}}
        ],
        sinks={"console": console_sink}
    )

    processor = ObservabilityDataProcessor(processor_config)
    processor.start()

    test_metrics = [
        {"name": "cpu_usage", "value": 0.75, "timestamp": 1678886400.0, "tags": {"host": "server-1", "service": "web-app"}},
        {"name": "memory_free", "value": 1024.5, "timestamp": 1678886401.0, "tags": {"host": "server-2", "service": "db-svc"}},
    ]
    test_logs = [
        {"message": "User login failed", "level": "WARNING", "timestamp": 1678886402.0, "service": "auth-svc", "context": {"user": "bad_user"}},
        {"message": "Legacy system error", "level": "ERROR", "timestamp": 1678886403.0, "service": "legacy-app", "context": {"code": 500}}, # Should be filtered
        {"message": "Database connection lost", "level": "ERROR", "timestamp": 1678886404.0, "service": "db-svc", "context": {"reason": "timeout"}},
    ]

    for metric in test_metrics:
        await processor.ingest_metric(metric)
    for log in test_logs:
        await processor.ingest_log_entry(log)

    await asyncio.sleep(6) # Allow some time for flushing
    await processor.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")