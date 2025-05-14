class EventUtils:
    def __init__(self, s3_service, sqs_service, config, logger, perform_deduplication=True):
        """
        Initializes the EventUtils with required services and configuration.

        Args:
            s3_service: Service used to interact with S3
            sqs_service: Service used to interact with SQS.
            config (dict): Configuration dictionary containing required parameters.
            logger: Logger instance for logging messages.
            perform_deduplication (bool): Whether to perform deduplication
        """
        self.s3_service = s3_service
        self.sqs_service = sqs_service
        self.raw_bucket = config["RAW_DATA_BUCKET"]
        self.logger = logger
        self.perform_deduplication = perform_deduplication
        
        # For tracking stats
        self.processed_count = 0
        self.duplicate_count = 0

    def process_event(self, event):
        """
        Processes an S3-triggered event, validating and delegating each record
        for processing. Performs deduplication if configured.

        Args:
            event (dict): The event payload from S3/Lambda trigger.
            
        Returns:
            tuple: Count of (processed_records, duplicate_records)
        """
        self.logger.info("🟢 Starting event processing.")
        
        # Reset counters for this event
        self.processed_count = 0
        self.duplicate_count = 0
        
        # Extract list of records from the event
        records = event.get("Records", [])
        
        # Raise error if no records are found
        if not records:
            raise ValueError("No records found in event.")
        
        # Process each individual S3 event record
        for record in records:
            self._process_record(record)

        self.logger.info(
            f"🟢 Event processing completed successfully. Processed {self.processed_count} "
            f"records, skipped {self.duplicate_count} duplicates.")
        
        return (self.processed_count, self.duplicate_count)

    def _process_record(self, record):
        """
        Processes a single S3 event record by validating and extracting CSV data.
        Performs deduplication if configured.

        Args:
            record (dict): An individual S3 event record.
        """
        
        # Extract bucket name and file key from the event
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        self.logger.info(f"🔄 Triggered by file: s3://{bucket}/{key}")

        # Skip if the file is from a different bucket
        if bucket != self.raw_bucket:
            self.logger.info(f"🚫 Ignoring file from non-target bucket: {bucket}")
            return
            
        # Skip already processed files
        if key.startswith("processed/"):
            self.logger.info(f"ℹ️ Skipping already processed file: {key}")
            return

        # Skip files that are not CSV
        if not key.endswith(".csv"):
            self.logger.info(f"❌ Skipped non-CSV file: {key}")
            return

        # Log and start extracting and sanitizing CSV data
        self.logger.info(f"🔄 Starting extraction from {key}")
        sanitized_data = self.s3_service.extract_csv(bucket, key, self.perform_deduplication)
        
        # Update duplicate count from S3 service
        self.duplicate_count += self.s3_service.last_duplicate_count
        
        # Log the extraction results
        if self.perform_deduplication:
            self.logger.info(
                f"✅ Extracted and sanitized {len(sanitized_data)} unique rows from {key} "
                f"(skipped {self.s3_service.last_duplicate_count} duplicates)"
            )
        else:
            self.logger.info(
                f"✅ Extracted and sanitized {len(sanitized_data)} rows from {key}"
            )

        # If there's data, send it to the SQS queue for further processing
        if sanitized_data:
            self.logger.info(f"🔜 Sending {len(sanitized_data)} records to SQS as a batch.")
            self.sqs_service.send_message(sanitized_data)
            self.processed_count += len(sanitized_data)
        else:
            self.logger.info("ℹ️ No unique data to send to SQS.")