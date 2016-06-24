BROKER_URL = 'amqp://couchbase:couchbase@ci.sc.couchbase.com:5672/broker'
CELERY_RESULT_BACKEND = 'amqp'
CELERY_RESULT_EXCHANGE = 'perf_results'
CELERY_RESULT_PERSISTENT = False
