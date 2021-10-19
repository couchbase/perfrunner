from kombu.serialization import registry

broker_url = 'sqla+sqlite:///perfrunner.db'
result_backend = 'database'
database_url = 'sqlite:///results.db'
task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = {'pickle', 'json', 'application/json', 'application/data', 'application/text'}
task_protocol = 1

registry.enable('json')
registry.enable('application/json')
registry.enable('application/data')
registry.enable('application/text')
