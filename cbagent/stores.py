import json
from typing import Dict, List

from requests import Session


class PerfStore:

    def __init__(self, host: str):
        self.session = Session()
        self.async_session = None
        self.base_url = 'http://{}:8080'.format(host)
        self.dbs = set()

    @staticmethod
    def build_dbname(cluster: str,
                     server: str = None,
                     bucket: str = None,
                     index: str = None,
                     collector: str = None) -> str:
        db_name = (collector or "") + cluster + (bucket or "") + (index or "") + (server or "")
        for char in "[ ] / \\ ; . , > < & * : % = + @ ! # ^ ( ) | ? ^ ' \"":
            db_name = db_name.replace(char, "")
        return db_name

    def push(self, db: str, data: dict, timestamp: str):
        url = '{}/{}'.format(self.base_url, db)
        if timestamp is not None:
            url = '{}?ts={}'.format(url, timestamp)
        self.session.post(url=url, data=json.dumps(data))

    async def async_push(self, db: str, data: dict, timestamp: str):
        url = '{}/{}'.format(self.base_url, db)
        if timestamp is not None:
            url = '{}?ts={}'.format(url, timestamp)
        async with self.async_session.post(url=url, json=data) as response:
            return await response.json()

    def get_timeseries(self, db: str, metric: str) -> list[list[int, float]]:
        url = f"{self.base_url}/{db}/{metric}"
        data = self.session.get(url).json()
        return data

    def get_values(self, db: str, metric) -> list[float]:
        return [v for _, v in self.get_timeseries(db, metric)]

    def bulk_get_timeseries(self, dbs: list[str], metric: str) -> list[list[list[int, float]]]:
        return [self.get_timeseries(db, metric) for db in dbs]

    def bulk_get_timeseries_merged(self, dbs: list[str], metric: str) -> list[list[int, float]]:
        return sorted(
            [
                ts_value
                for timeseries in self.bulk_get_timeseries(dbs, metric)
                for ts_value in timeseries
            ],
            key=lambda x: x[0],
        )

    def get_summary(self, db: str, metric: str) -> Dict[str, float]:
        url = '{}/{}/{}/summary'.format(self.base_url, db, metric)
        return self.session.get(url).json()

    def exists(self, db: str, metric: str) -> bool:
        url = '{}/{}/{}'.format(self.base_url, db, metric)
        response = self.session.get(url)
        return response.status_code == 200

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

    async def append_async(self, data, cluster=None, server=None, bucket=None,
                           index=None, collector=None, timestamp=None):
        db = self.build_dbname(cluster, server, bucket, index, collector)
        return await self.async_push(db, data, timestamp)
