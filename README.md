# cloudwrapper

Wrappers around cloud services for Amazon, Google and private cloud.

**Copyright**: (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)

**Maintainer**: Martin Mikita, martin.mikita@klokantech.com

## Modules

 - Amazon cloud services:
   - *s3*: Amazon S3 storage.
   - *sqs*: Amazon SQS queues.
   - *cwl*: Amazon CloudWatch logs.

 - Google cloud services:
   - *gce*: Google Compute Engine instance metadata.
   - *gcl*: Google Cloud Logging.
   - *gcm*: Google Custom Metric (v2).
   - *gcm3*: Google Custom Metric (v3).
   - *gcs*: Google Cloud Storage.
   - *gdm*: Google Deployment Manager using API v2.
   - *gps*: Google PubSub using GCE Authentication.
   - *gtq*: Google Task Pull Queues.

 - Other cloud services:
   - *btq*: BeansTalkd Queues.
   - *idm*: Influx DB Metric.


## Install

*WARNING*: Cloudwrapper uses new `requests` module, while old `pip` (from apt-get on Ubuntu 14.04, Debian 8) requires older `requests` module.

URL for master (latest) version:

```
https://github.com/klokantech/cloudwrapper/archive/master.zip
```

### Versions

 - v1.1 : `https://github.com/klokantech/cloudwrapper/archive/v1.1.zip`
 - v1.0 : `https://github.com/klokantech/cloudwrapper/archive/v1.0.zip`



### Python2

```bash
apt-get install python-setuptools
easy_install pip
pip install https://github.com/klokantech/cloudwrapper/archive/master.zip
```

### Python3

```bash
apt-get install python3-setuptools
easy_install3 pip
pip3 install https://github.com/klokantech/cloudwrapper/archive/master.zip
```


## Usage example

### BeansTalkd Queues

```python
from cloudwrapper.btq import BtqConnection

btq = BtqConnection('172.17.0.2', 11300)  # host, port

q = btq.queue('test')  # get test queue object

print (q.qsize())  # print size of the queue test
```
