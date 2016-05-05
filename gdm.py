"""
Google Cloud Deployment Manager using API v2.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import json
import errno
import datetime
import yaml


from time import sleep
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError

from .gce import GoogleComputeEngine


class GdmConnection(object):

    def __init__(self):
        self.credentials = GoogleCredentials.get_application_default()
        self.client_dm = build('deploymentmanager', 'v2', credentials=self.credentials)
        self.client_ce = build('compute', 'v1', credentials=self.credentials)


    def deployment(self, name, projectId=None):
        return Deployment(name, projectId, self.client_dm, self.client_ce, self.credentials)


class Deployment(object):

    def _format_rfc3339(self, dt):
        """Formats a datetime per RFC 3339.
        :param dt: Datetime instanec to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, name, projectId, client_dm, client_ce, credentials):
        self.deploymentName = name
        self.gce = GoogleComputeEngine()
        if projectId is None:
            #projectId = 'projects/' + self.gce.projectId()
            projectId = self.gce.projectId()
        elif 'projects/' in projectId:
            projectId = projectId[10:]
        # elif 'projects/' not in projectId:
            # projectId = 'projects/' + projectId
        self.zone = str(self.gce.instanceZone())
        self.projectId = projectId
        self.client_dm = client_dm
        self.client_ce = client_ce
        self.credentials = credentials
        self.resources = []
        self.imports = []


    def _reconnect(self):
        self.credentials = GoogleCredentials.get_application_default()
        self.client_dm = build('deploymentmanager', 'v2', credentials=self.credentials)
        self.client_ce = build('compute', 'v1', credentials=self.credentials)
        self.gce = GoogleComputeEngine()


    def setZone(self, zone):
        self.zone = zone


    def name(self):
        return self.deploymentName


    def create(self, preview=None):
        content = {"resources": self.resources}


        body = {
            "target": {
                "config": {
                    "content": yaml.dump(content, default_flow_style=False)
                }
            },
            "name": self.deploymentName
        }
        try:
            if self.has():
                deploymentState = self.get()
                fingerprint = deploymentState.get('fingerprint')
                body.update({"fingerprint": fingerprint})
                response = self.client_dm.deployments().update(
                    project=self.projectId,
                    deployment=self.deploymentName,
                    body=body,
                    preview=preview
                ).execute(num_retries=6)
            else:
                response = self.client_dm.deployments().insert(
                    project=self.projectId,
                    body=body,
                    preview=preview
                ).execute(num_retries=6)
        except Exception as ex:
           raise Exception('Failed to create deployment {}: {}'.format(self.deploymentName, ex))

        return response


    def get(self):
        try:
            request = self.client_dm.deployments().get(
                project=self.projectId,
                deployment=self.deploymentName)
            response = request.execute(num_retries=6)
            return response
        except HttpError as ex:
            if ex.resp.status == 404:
                return None
            raise
        return None


    def has(self):
        try:
            response = self.get()
        except Exception:
            pass
        return True if response is not None else False


    def delete(self):
        try:
            response = self.client_dm.deployments().delete(
                project=self.projectId,
                deployment=self.deploymentName
            ).execute(num_retries=6)
            return response
        except Exception:
            pass
        return None


    def addResource(self, resource):
        self.resources.append(resource)


    def addInstanceManagedGroup(self, name, template, description=None, targetSize=0):
        properties = {
            "baseInstanceName": name,
            "instanceTemplate": "projects/{}/global/instanceTemplates/{}".format(
                self.projectId, template),
            "name": name,
            "targetSize": targetSize,
            "zone": self.zone
        }
        resource = {
            "name": name,
            "type": "compute.v1.instanceGroupManager",
            "properties": properties
        }
        self.addResource(resource)


    def addInstanceManagedAutoscaler(self, name, groupName, numRange, coolDown=300, utilization=None):
        if utilization is None:
            # Set default CPU utilization to 0.8 value
            utilization = {
                "cpuUtilization": {
                    "utilizationTarget": 0.8
                }
            }
        properties = {
            "target": "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/instanceGroupManagers/{}".format(
                self.projectId, self.zone, groupName),
            "autoscalingPolicy": {
                "minNumReplicas": numRange[0],
                "maxNumReplicas": numRange[1],
                "coolDownPeriodSec": coolDown,
            },
            "zone": self.zone
        }
        properties["autoscalingPolicy"].update(utilization)
        resource = {
            "name": name,
            "type": "compute.v1.autoscaler",
            "properties": properties
        }
        self.addResource(resource)


    def addInstanceManagedAutoscalerCustomMetric(self, name, groupName, numRange,
        metricName, metricTarget, metricTargetType, coolDown=300):
        utilization = {
            "customMetricUtilizations": [
                {
                    "metric": metricName,
                    "utilizationTarget": metricTarget,
                    "utilizationTargetType": metricTargetType
                }
            ]
        }
        self.addInstanceManagedAutoscaler(name, groupName, numRange, coolDown, utilization)


    def runningInstances(self, groupName):
        if not self.has():
            return 0
        try:
            request = self.client_ce.instanceGroupManagers().get(
                project=self.projectId,
                instanceGroupManager=groupName,
                zone=self.zone)
            response = request.execute(num_retries=6)
        except HttpError as ex:
            if ex.resp.status == 404:
                return 0
            raise
        size = int(response.get('targetSize'))
        return size

