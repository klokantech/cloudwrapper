# cloudwrapper

Wrappers around cloud services for Amazon, Google and private cloud.

**Copyright**: (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)

**Maintainer**: Martin Mikita, martin.mikita@klokantech.com

## Modules

 - Amazon cloud services:
   - *s3*: Amazon S3 storage.
   - *sqs*: Amazon SQS queues.
   - *cwl*: Amazon CloudWatch logs.

   - **required packages**:
      - `boto==2.39.0`

 - Google cloud services:
   - *gce*: Google Compute Engine instance metadata.
   - *gcl*: Google Cloud Logging.
   - *gcm*: Google Custom Metric (v2).
   - *gcm3*: Google Custom Metric (v3).
   - *gcs*: Google Cloud Storage.
   - *gdm*: Google Deployment Manager using API v2.
   - *gps*: Google PubSub using GCE Authentication.
   - *gtq*: Google Task Pull Queues.

   - **required packages**:
      - `requests==2.9.1`
      - `gcloud==0.13.0`
      - `oauth2client==2.0.2`
      - `google-api-python-client==1.5.1`
      - `gcloud_taskqueue==0.1.2`
      - `pyyaml==3.11`

 - BeansTalkd:
   - *btq*: BeansTalkd Queues.

   - **required packages**:
      - `pyyaml==3.11`
      - `beanstalkc3==0.4.0`

 - InfluxDB:
   - *idm*: Influx DB Metric.

   - **required packages**:
      - `influxdb==3.0.0`


## Install

*WARNING*: Cloudwrapper uses new `requests` module, while old `pip` (from apt-get on Ubuntu 14.04, Debian 8) requires older `requests` module.

URL for master (latest) version:

```
https://github.com/klokantech/cloudwrapper/archive/master.zip
```

### Versions

 - v1.2 : `https://github.com/klokantech/cloudwrapper/archive/v1.2.zip`
 - v1.1 : `https://github.com/klokantech/cloudwrapper/archive/v1.1.zip`
 - v1.0 : `https://github.com/klokantech/cloudwrapper/archive/v1.0.zip`



### Python2

```bash
apt-get install python-setuptools
easy_install pip
pip install https://github.com/klokantech/cloudwrapper/archive/v1.2.zip
```

### Python3

```bash
apt-get install python3-setuptools
easy_install3 pip
pip3 install https://github.com/klokantech/cloudwrapper/archive/v1.2.zip
```


## Usage example


### BeansTalkd Queues

Install required packages: `pip install beanstalkc3==0.4.0 pyyaml==3.11`

```python
from cloudwrapper.btq import BtqConnection

btq = BtqConnection('172.17.0.2', 11300)  # host, port

q = btq.queue('test')  # get test queue object

print (q.qsize())  # print size of the queue test
```
