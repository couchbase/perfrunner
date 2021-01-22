from setuptools import setup, Extension

fastdocgen = Extension('fastdocgen', sources=['spring/fastdocgen.c'])

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': [
            'clients = perfrunner.utils.clients:main',
            'cloudrunner = perfrunner.utils.cloudrunner:main',
            'cluster = perfrunner.utils.cluster:main',
            'debug = perfrunner.utils.debug:main',
            'deploy = perfrunner.utils.deploy:main',
            'destroy = perfrunner.utils.destroy:main',
            'go_dependencies = perfrunner.utils.go_dependencies:main',
            'jenkins = perfrunner.utils.jenkins:main',
            'hidefast = perfrunner.utils.hidefast:main',
            'install = perfrunner.utils.install:main',
            'setup = perfrunner.utils.setup:main',
            'perfrunner = perfrunner.__main__:main',
            'recovery = perfrunner.utils.recovery:main',
            'spring = spring.__main__:main',
            'stats = perfrunner.utils.stats:main',
            'templater = perfrunner.utils.templater:main',
            'trigger = perfrunner.utils.trigger:main',
            'verify_logs = perfrunner.utils.verify_logs:main',
            'weekly = perfrunner.utils.weekly:main',
        ],
    },
    ext_modules=[
        fastdocgen
    ],
)
