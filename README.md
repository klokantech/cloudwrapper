# cloudwrapper

Wrappers around cloud services for Amazon, Google and private cloud.

**Copyright**: (C) 2016-2018 Klokan Technologies GmbH (http://www.klokantech.com/)

**Maintainer**: Martin Mikita, martin.mikita@klokantech.com

## Modules

 - Amazon cloud services:
   - *s3*: Amazon S3 storage.
   - *sqs*: Amazon SQS queues.
   - *cwl*: Amazon CloudWatch logs.
   - **required packages**:
      - `boto==2.48.0`

 - Google cloud services:
   - *gce*: Google Compute Engine instance metadata.
   - *gcl*: Google Cloud Logging.
   - *gcm*: Google Custom Metric (v2) [**DEPRECATED**](https://github.com/klokantech/cloudwrapper/issues/13).
   - *gcs*: Google Cloud Storage.
   - *gdm*: Google Deployment Manager using API v2.
   - *gps*: Google PubSub using GCE Authentication.
   - *gtq*: Google Task Pull Queues.
   - **required packages**:
      - `gcloud==0.13.0`
      - `gcloud_taskqueue==0.1.2`
      - `google-api-python-client==1.5.1`
      - `oauth2client==2.0.2`
      - `pyyaml==3.11`
      - `requests==2.9.1`

   - *gcm3*: Google Custom Metric (v3).
   - **required packages**:
      - `google-cloud-monitoring==0.27.0`
      - `google-cloud-core==0.27.1`
      - `oauth2client==2.0.2`
      - `requests==2.18.4`

 - BeansTalkd:
   - *btq*: BeansTalkd Queues.
   - **required packages**:
      - `beanstalkc3==0.4.0`
      - `pyyaml==3.11`

 - InfluxDB:
   - *idm*: Influx DB Metric.
   - *idl*: Influx DB Logging.
   - *idb*: Influx DB direct use (as SQL database).
   - **required packages**:
      - `influxdb==3.0.0`


## Install

*WARNING*: Cloudwrapper uses new `requests` module, while old `pip` (from apt-get on Ubuntu 14.04, Debian 8) requires older `requests` module.

The URL link for the master (latest) version:

```
https://github.com/klokantech/cloudwrapper/archive/master.zip
```

### Versions

The latest released versions:

 - v1.20 : `https://github.com/klokantech/cloudwrapper/archive/v1.20.zip`
 - v1.19 : `https://github.com/klokantech/cloudwrapper/archive/v1.19.zip`
 - v1.18 : `https://github.com/klokantech/cloudwrapper/archive/v1.18.zip`
 - v1.17 : `https://github.com/klokantech/cloudwrapper/archive/v1.17.zip`
 - v1.16 : `https://github.com/klokantech/cloudwrapper/archive/v1.16.zip`
 - v1.15 : `https://github.com/klokantech/cloudwrapper/archive/v1.15.zip`

The list of all released versions are in the [Releases](https://github.com/klokantech/cloudwrapper/releases) section of this repository.
The link for older version has this format: `https://github.com/klokantech/cloudwrapper/archive/vX.Y.zip`, where `vX.Y` is the tag on this repository.


### Python2

```bash
apt-get install python-setuptools
easy_install pip
pip install https://github.com/klokantech/cloudwrapper/archive/v1.20.zip
```

### Python3

```bash
apt-get install python3-setuptools
easy_install3 pip
pip3 install https://github.com/klokantech/cloudwrapper/archive/v1.20.zip
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
