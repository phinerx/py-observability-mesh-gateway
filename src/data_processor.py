import logging
from typing import Dict, Any, List, Union
import time

# Configure basic logging for the module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ObservabilityDataProcessor:
    """
    A foundational component for ingesting, transforming, and enriching observability data
    (logs, metrics, traces) within the mesh gateway. This processor applies configurable
    rules to standardize, filter, and augment incoming data before routing.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the data processor with a given configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing processing rules,
                                     enrichment sources, and filtering criteria.
                                     Expected keys:
                                     - 'enrichment_rules': List of Dicts for data augmentation.
                                     - 'filtering_rules': List of Dicts for data exclusion.
                                     - 'transformation_maps': Dict for field remapping.
                                     - 'batch_size': Integer for batch processing limits.
        """
        self.config = config
        self.enrichment_rules = config.get('enrichment_rules', [])
        self.filtering_rules = config.get('filtering_rules', [])
        self.transformation_maps = config.get('transformation_maps', {})
        self.batch_size = config.get('batch_size', 100)
        logger.info("ObservabilityDataProcessor initialized with configuration.")
        logger.debug(f"Processor configuration: {config}")

    def _apply_transformation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Applies field remapping and type conversions based on configured transformation maps."""
        transformed_data = data.copy()
        for old_key, new_key in self.transformation_maps.items():
            if old_key in transformed_data:
                transformed_data[new_key] = transformed_data.pop(old_key)
        # Example: Ensure 'timestamp' is in a consistent format or type
        if 'timestamp' in transformed_data and not isinstance(transformed_data['timestamp'], float):
            try:
                # Attempt to convert various timestamp formats to a Unix epoch float
                if isinstance(transformed_data['timestamp'], (int, str)):
                    # Basic conversion, more robust parsing needed for real-world
                    transformed_data['timestamp'] = float(transformed_data['timestamp'])
                else:
                    logger.warning(f"Unsupported timestamp format for conversion: {type(transformed_data['timestamp'])}")
            except (ValueError, TypeError):
                logger.error(f"Failed to convert timestamp: {transformed_data['timestamp']}")
        return transformed_data

    def _apply_enrichment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enriches the data point with additional context based on defined rules."""
        enriched_data = data.copy()
        for rule in self.enrichment_rules:
            field = rule.get('field')
            source = rule.get('source') # e.g., 'static', 'lookup_service'
            value = rule.get('value')

            if field and source == 'static':
                enriched_data[field] = value
            elif field and source == 'lookup_service':
                # This would typically involve an external call or a local cache lookup
                # For demonstration, we'll simulate a lookup
                lookup_key = rule.get('lookup_key', field)
                if lookup_key in enriched_data:
                    simulated_value = f"lookup_result_{enriched_data[lookup_key]}"
                    enriched_data[field] = simulated_value
                    logger.debug(f"Enriched '{field}' with simulated lookup value for key '{lookup_key}'.")
                else:
                    logger.warning(f"Lookup key '{lookup_key}' not found in data for enrichment rule.")
        return enriched_data

    def _apply_filtering(self, data: Dict[str, Any]) -> bool:
        """Determines if a data point should be filtered out based on configured rules."""
        for rule in self.filtering_rules:
            field = rule.get('field')
            operator = rule.get('operator')
            value = rule.get('value')

            if field in data:
                data_value = data[field]
                if operator == 'equals' and data_value == value:
                    logger.debug(f"Filtered out data: field '{field}' equals '{value}'.")
                    return True
                if operator == 'contains' and isinstance(data_value, str) and value in data_value:
                    logger.debug(f"Filtered out data: field '{field}' contains '{value}'.")
                    return True
                if operator == 'greater_than' and isinstance(data_value, (int, float)) and data_value > value:
                    logger.debug(f"Filtered out data: field '{field}' greater than '{value}'.")
                    return True
                # Add more operators as needed (e.g., less_than, not_equals, regex_match)
        return False # Not filtered

    def process_single_data_point(self, data_point: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        """
        Processes a single observability data point through the defined pipeline.

        Args:
            data_point (Dict[str, Any]): The raw log, metric, or trace data to process.

        Returns:
            Union[Dict[str, Any], None]: The processed and potentially enriched data point,
                                         or None if the data point was filtered out.
        """
        if not isinstance(data_point, dict):
            logger.error(f"Invalid data point type received: {type(data_point)}. Expected dict.")
            return None

        start_time = time.monotonic()
        processed_data = data_point.copy()

        try:
            # Step 1: Apply transformations
            processed_data = self._apply_transformation(processed_data)

            # Step 2: Apply enrichment
            processed_data = self._apply_enrichment(processed_data)

            # Step 3: Apply filtering
            if self._apply_filtering(processed_data):
                logger.info(f"Data point filtered out: {processed_data.get('id', 'N/A')}")
                return None

            end_time = time.monotonic()
            processing_duration = (end_time - start_time) * 1000 # milliseconds
            logger.debug(f"Successfully processed data point in {processing_duration:.2f}ms.")
            return processed_data
        except Exception as e:
            logger.error(f"Error processing data point '{data_point.get('id', 'N/A')}': {e}", exc_info=True)
            return None

    def process_batch(self, data_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes a batch of observability data points.

        Args:
            data_batch (List[Dict[str, Any]]): A list of raw data points to process.

        Returns:
            List[Dict[str, Any]]: A list of processed data points that passed filtering.
        """
        if not isinstance(data_batch, list):
            logger.error(f"Invalid data batch type received: {type(data_batch)}. Expected list.")
            return []

        processed_results = []
        for i, data_point in enumerate(data_batch):
            if i >= self.batch_size:
                logger.warning(f"Batch size limit ({self.batch_size}) exceeded. Skipping remaining items.")
                break
            result = self.process_single_data_point(data_point)
            if result is not None:
                processed_results.append(result)
        logger.info(f"Processed batch of {len(data_batch)} items. {len(processed_results)} items passed filtering.")
        return processed_results

# Example Usage (for testing/demonstration purposes)
if __name__ == "__main__":
    test_config = {
        'enrichment_rules': [
            {'field': 'region', 'source': 'static', 'value': 'us-east-1'},
            {'field': 'service_owner', 'source': 'lookup_service', 'lookup_key': 'service_name'}
        ],
        'filtering_rules': [
            {'field': 'level', 'operator': 'equals', 'value': 'DEBUG'},
            {'field': 'message', 'operator': 'contains', 'value': 'healthcheck'}
        ],
        'transformation_maps': {
            'ts': 'timestamp',
            'severity': 'level'
        },
        'batch_size': 3
    }

    processor = ObservabilityDataProcessor(test_config)

    sample_logs = [
        {'id': 'log-001', 'ts': 1678886400, 'severity': 'INFO', 'message': 'User login successful', 'service_name': 'auth-service'},
        {'id': 'log-002', 'ts': 1678886401, 'severity': 'DEBUG', 'message': 'Database query debug', 'service_name': 'db-service'}, # Should be filtered
        {'id': 'log-003', 'ts': '1678886402', 'severity': 'ERROR', 'message': 'Service unavailable', 'service_name': 'api-gateway'},
        {'id': 'log-004', 'ts': 1678886403, 'severity': 'INFO', 'message': 'Performing healthcheck', 'service_name': 'monitor-agent'}, # Should be filtered
        {'id': 'log-005', 'ts': 1678886404, 'severity': 'WARNING', 'message': 'High CPU usage', 'service_name': 'compute-node'}
    ]

    print("\n--- Processing single data points ---")
    for log in sample_logs:
        processed_log = processor.process_single_data_point(log)
        if processed_log:
            print(f"Processed: {processed_log}")
        else:
            print(f"Filtered: {log}")

    print("\n--- Processing a batch of data points ---")
    processed_batch = processor.process_batch(sample_logs)
    for log in processed_batch:
        print(f"Batch Processed: {log}")

    print("\n--- Testing with invalid data ---")
    invalid_data = "this is not a dict"
    processor.process_single_data_point(invalid_data)

    invalid_batch = ["item1", "item2"]
    processor.process_batch(invalid_batch)
