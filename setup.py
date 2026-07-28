from setuptools import Extension, setup

# The C extension is declared here rather than in pyproject.toml because the
# [tool.setuptools] ext-modules key requires setuptools >= 74.1, which is newer
# than what stock environments (e.g. GitHub runners) provide.
setup(ext_modules=[Extension("fastdocgen", sources=["spring/fastdocgen.c"])])
