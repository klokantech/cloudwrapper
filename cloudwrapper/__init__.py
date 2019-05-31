"""Wrappers around cloud services for Amazon, Google and private cloud.

Copyright (C) 2016-2019 Klokan Technologies GmbH (https://www.klokantech.com/)
Maintainer: Martin Mikita, martin.mikita@klokantech.com

Modules:

- Amazon cloud services:
s3 -- Amazon S3 storage.
sqs -- Amazon SQS queues.
cwl -- Amazon CloudWatch logs.

- Google cloud services:
gce -- Google Compute Engine instance metadata.
gcl -- Google Cloud Logging.
gcm -- Google Custom Metric (v2).
gcm3 -- Google Custom Metric (v3).
gcs -- Google Cloud Storage.
gdm -- Google Deployment Manager using API v2.
gps -- Google PubSub using GCE Authentication.
gtq -- Google Task Pull Queues.

- Other cloud services:
btq -- BeansTalkd Queues.

idm -- Influx DB Metric.
idl -- Influx DB Logging.
idb -- Influx DB direct use (as SQL database).

"""

__version__ = '1.24'
