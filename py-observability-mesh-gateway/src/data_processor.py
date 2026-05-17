import json
import time
from typing import Dict, Any, List, Optional

class ObservabilityDataProcessor:
    """
    A robust processor for observability data, handling metrics, logs, and traces.
    It applies configurable transformations, enrichment, and filtering rules.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the data processor with a configuration dictionary.
        :param config: Dictionary containing processing rules, enrichment sources, etc.
        """
        self.config = config
        self.metric_rules = config.get("metric_processing_rules", [])
        self.log_rules = config.get("log_processing_rules", [])
        self.trace_rules = config.get("trace_processing_rules", [])
        self.enrichment_sources = self._load_enrichment_sources(config.get("enrichment_config", {}))
        self.filter_rules = config.get("global_filter_rules", [])

    def _load_enrichment_sources(self, enrichment_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Loads and prepares external data sources for enrichment.
        This could involve database connections, API clients, or cached data.
        For demonstration, we'll simulate a static lookup.
        """
        sources = {}
        # Example: Simulate loading a service metadata map
        if "service_metadata_path" in enrichment_config:
            try:
                with open(enrichment_config["service_metadata_path"], "r") as f:
                    sources["service_metadata"] = json.load(f)
            except FileNotFoundError:
                print(f"Warning: Service metadata file not found at {enrichment_config['service_metadata_path']}")
                sources["service_metadata"] = {}
        else:
            sources["service_metadata"] = {
                "service_a": {"owner": "team_alpha", "criticality": "high"},
                "service_b": {"owner": "team_beta", "criticality": "medium"}
            }
        return sources

    def _apply_metric_rules(self, metric: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Applies configured rules to a single metric."""
        # Example: Add a timestamp if missing, normalize tags
        if "timestamp" not in metric:
            metric["timestamp"] = int(time.time() * 1000) # Milliseconds
        
        # Example: Apply custom transformations based on metric_rules
        for rule in self.metric_rules:
            if rule["type"] == "rename_tag" and rule["old_tag"] in metric.get("tags", {}):
                metric["tags"][rule["new_tag"]] = metric["tags"].pop(rule["old_tag"])
            elif rule["type"] == "add_default_tag" and rule["tag_key"] not in metric.get("tags", {}):
                metric.setdefault("tags", {})[rule["tag_key"]] = rule["tag_value"]
        
        return metric

    def _apply_log_rules(self, log_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Applies configured rules to a single log entry."""
        # Example: Parse JSON log messages, add source context
        if "message" in log_entry and isinstance(log_entry["message"], str):
            try:
                parsed_message = json.loads(log_entry["message"])
                log_entry.update(parsed_message)
                del log_entry["message"] # Replace raw message with parsed fields
            except json.JSONDecodeError:
                pass # Not a JSON log, keep as is
        
        log_entry.setdefault("source_ip", "unknown") # Example enrichment
        
        # Apply custom transformations based on log_rules
        for rule in self.log_rules:
            if rule["type"] == "mask_sensitive" and rule["field"] in log_entry:
                log_entry[rule["field"]] = "***MASKED***"
        
        return log_entry

    def _apply_trace_rules(self, span: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Applies configured rules to a single trace span."""
        # Example: Ensure essential trace context fields are present
        span.setdefault("start_time_unix_nano", int(time.time() * 1_000_000_000))
        span.setdefault("end_time_unix_nano", int(time.time() * 1_000_000_000))
        
        # Apply custom transformations based on trace_rules
        for rule in self.trace_rules:
            if rule["type"] == "add_span_attribute" and rule["attribute_key"] not in span.get("attributes", {}):
                span.setdefault("attributes", {})[rule["attribute_key"]] = rule["attribute_value"]
        
        return span

    def _enrich_data(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enriches a data item with additional context from loaded sources.
        """
        service_name = data_item.get("service_name")
        if service_name and "service_metadata" in self.enrichment_sources:
            metadata = self.enrichment_sources["service_metadata"].get(service_name)
            if metadata:
                data_item.update({"enriched_service_metadata": metadata})
        return data_item

    def _apply_global_filters(self, data_item: Dict[str, Any]) -> bool:
        """
        Applies global filtering rules to a data item.
        Returns True if the item should be kept, False otherwise.
        """
        for rule in self.filter_rules:
            if rule["type"] == "drop_if_field_equals":
                field_value = data_item.get(rule["field"])
                if field_value == rule["value"]:
                    return False
            elif rule["type"] == "drop_if_field_contains":
                field_value = data_item.get(rule["field"])
                if isinstance(field_value, str) and rule["substring"] in field_value:
                    return False
            # Add more complex filtering logic here (regex, numerical comparisons)
        return True

    def process_metrics(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Processes a list of metric data points."""
        processed_data = []
        for metric in metrics:
            item = self._apply_metric_rules(metric)
            if item:
                item = self._enrich_data(item)
                if self._apply_global_filters(item):
                    processed_data.append(item)
        return processed_data

    def process_logs(self, log_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Processes a list of log entries."""
        processed_data = []
        for log_entry in log_entries:
            item = self._apply_log_rules(log_entry)
            if item:
                item = self._enrich_data(item)
                if self._apply_global_filters(item):
                    processed_data.append(item)
        return processed_data

    def process_traces(self, spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Processes a list of trace spans."""
        processed_data = []
        for span in spans:
            item = self._apply_trace_rules(span)
            if item:
                item = self._enrich_data(item)
                if self._apply_global_filters(item):
                    processed_data.append(item)
        return processed_data

    def process_batch(self, batch: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Processes a mixed batch of observability data.
        :param batch: A dictionary with keys like 'metrics', 'logs', 'traces',
                      each mapping to a list of corresponding data items.
        :return: A dictionary of processed data, maintaining the original structure.
        """
        processed_batch = {}
        if "metrics" in batch:
            processed_batch["metrics"] = self.process_metrics(batch["metrics"])
        if "logs" in batch:
            processed_batch["logs"] = self.process_logs(batch["logs"])
        if "traces" in batch:
            processed_batch["traces"] = self.process_traces(batch["traces"])
        return processed_batch

# Example Usage (for demonstration, not part of the file content)
if __name__ == "__main__":
    processor_config = {
        "metric_processing_rules": [
            {"type": "add_default_tag", "tag_key": "env", "tag_value": "production"}
        ],
        "log_processing_rules": [
            {"type": "mask_sensitive", "field": "user_id"}
        ],
        "global_filter_rules": [
            {"type": "drop_if_field_equals", "field": "status", "value": "debug"}
        ],
        "enrichment_config": {
            # "service_metadata_path": "./service_metadata.json" # Uncomment to load from file
        }
    }

    processor = ObservabilityDataProcessor(processor_config)

    sample_metrics = [
        {"name": "cpu_usage", "value": 0.75, "service_name": "service_a", "tags": {"host": "server1"}},
        {"name": "memory_free", "value": 1024, "service_name": "service_b", "tags": {"host": "server2"}}
    ]
    sample_logs = [
        {"timestamp": 1678886400, "level": "info", "message": "User login successful", "service_name": "service_a", "user_id": "user123"},
        {"timestamp": 1678886401, "level": "debug", "message": "Debug message for testing", "service_name": "service_b"}
    ]
    sample_traces = [
        {"trace_id": "t1", "span_id": "s1", "operation_name": "http_request", "service_name": "service_a", "attributes": {"method": "GET"}},
        {"trace_id": "t2", "span_id": "s2", "operation_name": "db_query", "service_name": "service_b", "status": "ok"}
    ]

    processed_metrics = processor.process_metrics(sample_metrics)
    processed_logs = processor.process_logs(sample_logs)
    processed_traces = processor.process_traces(sample_traces)

    print("Processed Metrics:")
    for m in processed_metrics:
        print(json.dumps(m, indent=2))

    print("\nProcessed Logs:")
    for l in processed_logs:
        print(json.dumps(l, indent=2))

    print("\nProcessed Traces:")
    for t in processed_traces:
        print(json.dumps(t, indent=2))

    # Test batch processing
    sample_batch = {
        "metrics": sample_metrics,
        "logs": sample_logs,
        "traces": sample_traces
    }
    processed_batch = processor.process_batch(sample_batch)
    print("\nProcessed Batch:")
    print(json.dumps(processed_batch, indent=2))
