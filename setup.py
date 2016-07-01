from setuptools import setup, Extension

fastdocgen = Extension('fastdocgen', sources=['spring/fastdocgen.c'])

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': [
            'cbagent = cbagent.__main__:main',
            'spring = spring.__main__:main',
        ]
    },
    ext_modules=[
        fastdocgen
    ],
)
