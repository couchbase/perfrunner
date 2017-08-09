import json
from typing import List

from requests import Session


class PerfStore:

    def __init__(self, host: str):
        self.session = Session()
        self.base_url = 'http://{}:8080'.format(host)
        self.dbs = set()

    @staticmethod
    def build_dbname(cluster: str,
                     server: str = None,
                     bucket: str = None,
                     index: str = None,
                     collector: str = None) -> str:
        db_name = (collector or "") + cluster + (bucket or "") + (index or "") + (server or "")
        for char in "[]/\;.,><&*:%=+@!#^()|?^'\"":
            db_name = db_name.replace(char, "")
        return db_name

    def push(self, db: str, data: dict, timestamp: str):
        url = '{}/{}'.format(self.base_url, db)
        if timestamp is not None:
            url = '{}?ts={}'.format(url, timestamp)
        self.session.post(url=url, data=json.dumps(data))

    def get_values(self, db: str, metric) -> List[float]:
        url = '{}/{}/{}'.format(self.base_url, db, metric)

        data = self.session.get(url).json()
        return [d[1] for d in data]

    def find_dbs(self, db: str) -> List[str]:
        urls = []
        for name in self.session.get(self.base_url).json():
            if db in name:
                url = '{}/{}'.format(self.base_url, name)
                urls.append(url)
        return urls

    def append(self, data, cluster=None, server=None, bucket=None, index=None,
               collector=None, timestamp=None):
        db = self.build_dbname(cluster, server, bucket, index, collector)
        self.push(db, data, timestamp)
