from setuptools import setup, Extension

fastdocgen = Extension('fastdocgen', sources=['spring/fastdocgen.c'])

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': [
            'cbagent = cbagent.__main__:main',
            'cluster = perfrunner.utils.cluster:main',
            'debug = perfrunner.utils.debug:main',
            'install = perfrunner.utils.install:main',
            'perfrunner = perfrunner.__main__:main',
            'spring = spring.__main__:main',
        ],
    },
    ext_modules=[
        fastdocgen
    ],
)
