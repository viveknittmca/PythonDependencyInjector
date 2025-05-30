adapters/
├── neutral/
│   ├── api/
│   │   ├── base_client.py        # Handles session, retry, trace ID
│   │   └── client_metrics.py     # Prometheus metrics wrapper
│   ├── db/
│   │   └── relational_db_client.py  # SQLAlchemy-based DB interface
│   └── tracing/
│       └── tracer_logger.py      # Injects trace info into logs
├── cloud/
│   └── aws/
│       ├── s3/
│       │   ├── s3_client.py      # High-level methods (upload, list, etc.)
│       │   └── s3_region_factory.py  # Detect region + return client
│       └── sqs/
│           └── sqs_client.py     # SQS producer/consumer
└── platforms/
    ├── platform_a/
    │   ├── api_client.py         # Composes neutral.api_client + access token
    │   ├── rds_client.py         # Composes neutral.relational_db_client
    │   └── s3_client.py          # Wraps aws.s3_client with bucket logic
    └── platform_b/
        └── ...
