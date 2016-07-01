from setuptools import setup

setup(
    name='perfrunner',
    entry_points={
        'console_scripts': ['cbagent = cbagent.__main__:main']
    },
)
