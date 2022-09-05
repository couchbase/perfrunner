from typing import Dict, List

import requests
from decorator import decorator

from logger import logger


class InternalServerError(Exception):

    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "Internal server error: {}".format(self.url)


@decorator
def interrupt(request, *args, **kargs):
    try:
        return request(*args, **kargs)
    except (requests.ConnectionError, InternalServerError) as e:
        logger.interrupt(e)


class RestClient:

    def __init__(self):
        self.session = requests.Session()

    @interrupt
    def post(self, url, data):
        r = self.session.post(url=url, data=data)
        if r.status_code == 500:
            raise InternalServerError(url)

    @interrupt
    def get(self, url, params):
        r = self.session.get(url=url, params=params)
        if r.status_code == 500:
            raise InternalServerError(url)
        return r.json()


class MetadataClient(RestClient):

    def __init__(self, settings):
        super(MetadataClient, self).__init__()
        self.settings = settings
        self.base_url = "http://{}/cbmonitor".format(settings.cbmonitor_host)

    def get_clusters(self) -> List[str]:
        url = self.base_url + "/get_clusters/"
        return self.get(url, {})

    def get_servers(self) -> List[str]:
        url = self.base_url + "/get_servers/"
        params = {"cluster": self.settings.cluster}
        return self.get(url, params)

    def get_buckets(self) -> List[str]:
        url = self.base_url + "/get_buckets/"
        params = {"cluster": self.settings.cluster}
        return self.get(url, params)

    def get_indexes(self) -> List[str]:
        url = self.base_url + "/get_indexes/"
        params = {"cluster": self.settings.cluster}
        return self.get(url, params)

    def get_metrics(self, bucket: str = None, index: str = None,
                    server: str = None) -> List[Dict[str, str]]:
        url = self.base_url + "/get_metrics/"
        params = {"cluster": self.settings.cluster}
        for extra_param in "bucket", "index", "server":
            if (value := eval(extra_param)) is not None:
                if extra_param == "bucket" and value in self.settings.serverless_db_names:
                    value = self.settings.serverless_db_names[value]
                params[extra_param] = value
        return self.get(url, params)

    def add_cluster(self):
        if self.settings.cluster in self.get_clusters():
            return

        url = self.base_url + "/add_cluster/"
        data = {"name": self.settings.cluster}

        self.post(url, data)

    def add_server(self, address: str):
        if address in self.get_servers():
            return

        url = self.base_url + "/add_server/"
        data = {"address": address, "cluster": self.settings.cluster}

        self.post(url, data)

    def add_bucket(self, name: str):
        if name in self.settings.serverless_db_names:
            name = self.settings.serverless_db_names[name]

        if name in self.get_buckets():
            return

        url = self.base_url + "/add_bucket/"
        data = {"name": name, "cluster": self.settings.cluster}
        self.post(url, data)

    def add_index(self, name: str):
        if name in self.get_indexes():
            return

        url = self.base_url + "/add_index/"
        data = {"name": name, "cluster": self.settings.cluster}
        self.post(url, data)

    def add_metric(self, name: str, bucket: str = None, index: str = None,
                   server: str = None, collector: str = None):
        url = self.base_url + "/add_metric/"
        data = {"name": name, "cluster": self.settings.cluster}
        for extra_param in "bucket", "index", "server", "collector":
            if (value := eval(extra_param)) is not None:
                if extra_param == "bucket" and value in self.settings.serverless_db_names:
                    value = self.settings.serverless_db_names[value]
                data[extra_param] = value
        self.post(url, data)

    def add_snapshot(self, name: str):
        url = self.base_url + "/add_snapshot/"
        data = {"cluster": self.settings.cluster, "name": name}
        self.post(url, data)
