from setuptools import setup, find_packages
import sys

import cloudwrapper

with open('cloudwrapper/__init__.py') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            continue

open_kwds = {}
if sys.version_info > (3,):
    open_kwds['encoding'] = 'utf-8'

with open('README.md', **open_kwds) as f:
    readme = f.read()

install_requires = [
    'boto==2.39.0',
    'requests==2.9.1',
    'gcloud==0.13.0',
    'oauth2client==2.0.2',
    'google-api-python-client==1.5.1',
    'gcloud_taskqueue==0.1.2',
    'pyyaml==3.11',
    'beanstalkc3==0.4.0'
]

setup(
    name='cloudwrapper',
    version=cloudwrapper.__version__,
    description="Wrappers around cloud services for Amazon, Google and private cloud",
    long_description=readme,
    classifiers=[],
    keywords='',
    author='Klokan Technologies GmbH',
    author_email='info@klokantech.com',
    maintainer='Martin Mikita',
    maintainer_email='martin.mikita@klokantech.com',
    url='https://github.com/klokantech/cloudwrapper',
    license='Copyright 2016 Klokan Technologies GmbH',
    packages=find_packages(exclude=[]),
    include_package_data=True,
    install_requires=install_requires,
)
