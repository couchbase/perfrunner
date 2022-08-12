import datetime
import json
import os
import uuid

import boto3
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from fabric.api import local

from logger import logger


class CollectCloudwatch:

    def __init__(self, session, vol_ids, start_time, end_time):
        self.session = session
        self.vol_ids = vol_ids
        self.metric_reqs = self.all_metrics(vol_ids)
        self.start_time = start_time
        self.end_time = datetime.datetime.utcnow()

    def single(self, metric, stat, vol_id, metric_list):
        vol_id_sub = vol_id.replace("-", "_")
        metric_id = f'{vol_id_sub}_{metric}'
        metric_list.append({
            'Id': metric_id,
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EBS',
                    'MetricName': metric,
                    'Dimensions': [
                        {
                            'Name': 'VolumeId',
                            'Value': vol_id
                        },
                    ]
                },
                'Period': 60,
                'Stat': stat
            },
            'ReturnData': True
        })
        return metric_id

    def expression(self, metric, expr, vol_id, metric_list):
        # - is not a valid char for the IDs
        vol_id_sub = vol_id.replace("-", "_")
        metric_id = f'{vol_id_sub}_{metric}'
        metric_list.append({
            'Id': metric_id,
            'Expression': expr,
            'ReturnData': True,
            'Label': f'{vol_id} {metric}'
        })
        return metric_id

    def volume_metrics(self, vol_id):
        metric_list = []

        self.single("VolumeQueueLength", "Average", vol_id, metric_list)

        a = self.single("VolumeTotalReadTime", "Sum", vol_id, metric_list)
        b = vro = self.single("VolumeReadOps", "Sum", vol_id, metric_list)
        self.expression("AvgReadLatencyMs", f'IF({b} !=0, ({a} / {b}) * 1000, 0)', vol_id,
                        metric_list)

        a = self.single("VolumeTotalWriteTime", "Sum", vol_id, metric_list)
        vwo = self.single("VolumeWriteOps", "Sum", vol_id, metric_list)
        self.expression("AvgWriteLatencyMs", f'IF({vwo} !=0, ({a} / {vwo}) * 1000, 0)',
                        vol_id, metric_list)

        vwb = self.single("VolumeWriteBytes", "Sum", vol_id, metric_list)
        self.expression("AvgWriteSizeKiB", f'IF({vwo} != 0, ({vwb} / {vwo}) / 1024, 0)',
                        vol_id, metric_list)

        vrb = self.single("VolumeReadBytes", "Sum", vol_id, metric_list)
        self.expression("AvgReadSizeKiB", f'IF({vro} != 0, ({vrb} / {vro}) / 1024, 0)',
                        vol_id, metric_list)

        self.expression("ReadBandwidthKiBs", f'{vrb} / PERIOD({vrb}) / 1024', vol_id, metric_list)
        self.expression("WriteBandwidthKiBs", f'{vwb} / PERIOD({vwb}) / 1024', vol_id, metric_list)

        self.expression("WriteThroughputOpss", f'{vwo} / PERIOD({vwo})', vol_id, metric_list)
        self.expression("ReadThroughputOpss", f'{vro} / PERIOD({vro})', vol_id, metric_list)

        return metric_list

    def all_metrics(self, vol_ids):
        res = []
        for vol_id in vol_ids:
            res.extend(self.volume_metrics(vol_id))
        return res

    def dump(self, outf):
        return json.dump(self.get(), outf, default=datetime.datetime.timestamp)

    def get(self):
        cw = self.session.client("cloudwatch")
        cw_kwargs = dict(MetricDataQueries=self.metric_reqs,
                         StartTime=self.start_time,
                         EndTime=self.end_time,
                         ScanBy='TimestampAscending')

        reslist = [cw.get_metric_data(**cw_kwargs)]
        while "NextToken" in reslist[-1]:
            cw_kwargs["NextToken"] = reslist[-1]["NextToken"]
            reslist.append(cw.get_metric_data(**cw_kwargs))
        return reslist


class Cloudwatch:

    def __init__(self, cluster_ips, start_time, end_time, phase):
        username = "Administrator"
        password = "password"
        bucket_name = "cloudwatch"
        self.start_time = start_time
        self.end_time = end_time
        self.phase = phase

        auth = PasswordAuthenticator(username, password)

        cluster = Cluster('couchbase://perflab.sc.couchbase.com', ClusterOptions(auth))

        cb = cluster.bucket(bucket_name)

        cb_coll = cb.scope("EBS").collection("Volumes")

        volumes = []
        instances = []
        for ip in cluster_ips:
            logger.info(str(ip))
            i_id = local(
                "env/bin/aws ec2 describe-instances --filter Name=dns-name,Values={} "
                "--query \'Reservations[]."
                "Instances[].InstanceId\' --output text".format(
                    ip), capture=True)
            instances.append(i_id)
            volumes.append(local("env/bin/aws ec2 describe-volumes --filter Name=attachment."
                                 "instance-id,Values={} "
                                 "Name=attachment.device,Values=\"/dev/sdb\" "
                                 "--query \'Volumes[].Attachments[].VolumeId\' "
                                 "--output text".format(i_id),
                                 capture=True))

        record = {}
        for volume in volumes:
            cw = CollectCloudwatch(self.create_customized_session(), [volume], self.start_time,
                                   self.end_time)
            cw.dump(open('temp.json', "w"))
            with open('temp.json') as json_file:
                record[volume] = json.load(json_file)
        for volume in record:
            record[volume][0]['InstanceID'] = instances.pop(0)
            record[volume][0]['hostname'] = cluster_ips.pop(0)
            record[volume][0]['phase'] = self.phase
            del record[volume][0]['Messages']
            del record[volume][0]['ResponseMetadata']
        self.upsert_document(record, cb_coll, self.phase)

    def create_customized_session(self):
        session = boto3.session.Session(aws_access_key_id=os.getenv('AWS_ACCESS'),
                                        aws_secret_access_key=os.getenv('AWS_SECRET'),
                                        region_name=os.getenv('AWS_DEFAULT_REGION'))
        return session

    def upsert_document(self, doc, cb_coll, phase):
        try:
            key = 'snapshot-{}-{}'.format(phase, uuid.uuid4())
            cb_coll.upsert(key, doc)
            logger.info("Cloudwatch report available at: http://perflab.sc.couchbase.com:5000/"
                        "{}".format(key))
        except Exception as e:
            print(e)
