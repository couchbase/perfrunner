from setuptools import setup, Extension

fastdocgen = Extension('fastdocgen', sources=['spring/fastdocgen.c'])

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': [
            'cloudrunner = perfrunner.utils.cloudrunner:main',
            'cluster = perfrunner.utils.cluster:main',
            'debug = perfrunner.utils.debug:main',
            'go_dependencies = perfrunner.utils.go_dependencies:main',
            'hidefast = perfrunner.utils.hidefast:main',
            'install = perfrunner.utils.install:main',
            'perfrunner = perfrunner.__main__:main',
            'recovery = perfrunner.utils.recovery:main',
            'spring = spring.__main__:main',
            'templater = perfrunner.utils.templater:main',
            'trigger = perfrunner.utils.trigger:main',
            'verify_logs = perfrunner.utils.verify_logs:main',
        ],
    },
    ext_modules=[
        fastdocgen
    ],
)
