from kombu.serialization import registry

broker_url = 'amqp://couchbase:couchbase@172.23.96.202:5672/broker'
broker_pool_limit = None
worker_hijack_root_logger = False
result_backend = "rpc://"
result_persistent = False
result_exchange = "perf_results"
accept_content = ['pickle', 'json', 'application/json', 'application/data', 'application/text']
result_serializer = 'pickle'
task_serializer = 'pickle'
task_protocol = 2
broker_connection_timeout = 60
broker_connection_retry = True
broker_connection_max_retries = 100

registry.enable('json')
registry.enable('application/json')
registry.enable('application/data')
registry.enable('application/text')
