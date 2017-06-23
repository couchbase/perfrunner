from setuptools import setup, Extension

fastdocgen = Extension('fastdocgen', sources=['spring/fastdocgen.c'])

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': [
            'cluster = perfrunner.utils.cluster:main',
            'cloudrunner = perfrunner.utils.cloud.cloudrunner:main',
            'debug = perfrunner.utils.debug:main',
            'install = perfrunner.utils.install:main',
            'perfrunner = perfrunner.__main__:main',
            'spring = spring.__main__:main',
            'recovery = perfrunner.utils.recovery:main',
            'templater = perfrunner.utils.templater:main',
        ],
    },
    ext_modules=[
        fastdocgen
    ],
)
