SHELL := /bin/bash

PATH := ${GOPATH}/bin:$(PATH)

ENV := env
PYTHON := python3.6
PYTHON_PROJECTS := cbagent perfdaily perfrunner scripts spring
.PHONY: docker

all:
	export PYENV_ROOT="$$HOME/.pyenv" && \
	export PATH="$$PYENV_ROOT/bin:$$PATH" && \
	eval "$$(pyenv init -)" && \
	pyenv local 3.6.12 && \
	virtualenv --quiet --python ${PYTHON} ${ENV}
	${ENV}/bin/pip install --upgrade --quiet pip wheel
	${ENV}/bin/pip install --quiet --no-warn-script-location -r requirements.txt
	${ENV}/bin/python setup.py --quiet install
	pwd > ${ENV}/lib/${PYTHON}/site-packages/perfrunner.pth

clean:
	rm -fr build perfrunner.egg-info dist cachestat dcptest kvgen cbindexperf rachell loader *.db *.log .coverage *.pid celery
	find . -name '*.pyc' -o -name '*.pyo' -o -name __pycache__ | xargs rm -fr

pep8:
	${ENV}/bin/flake8 --statistics ${PYTHON_PROJECTS}
	${ENV}/bin/isort --quiet --check-only --recursive ${PYTHON_PROJECTS}
	${ENV}/bin/pydocstyle ${PYTHON_PROJECTS}

test:
	${ENV}/bin/nosetests -v --with-coverage --cover-package=cbagent,perfrunner,spring unittests.py

misspell:
	go get -u github.com/client9/misspell/cmd/misspell
	find go ${PYTHON_PROJECTS} -type f -not -name "*.txt" | xargs misspell -error

gofmt:
	gofmt -e -d -s go && ! gofmt -l go | read

check: pep8 misspell gofmt test

vendor-sync:
	go version
	go get -u github.com/kardianos/govendor
	govendor sync

cachestat:
	go build ./go/cachestats

dcptest: vendor-sync
	go build ./go/dcptest

buildquery: vendor-sync
	go get -u golang.org/x/tools/cmd/goyacc
	cd vendor/github.com/couchbase/query/parser/n1ql && sh build.sh

cbindexperf: buildquery
	go build ./go/cbindexperf

kvgen: vendor-sync
	go build ./go/kvgen

rachell:
	go build ./go/rachell

loader:
	go build ./go/loader

docker:
	docker build -t docker.io/perflab/perfrunner docker

docker-cloud-worker:
	pyenv local 3.6.12 && \
	virtualenv --quiet --python ${PYTHON} ${ENV}
	${ENV}/bin/pip install --upgrade --quiet pip wheel
	${ENV}/bin/pip install --quiet --no-warn-script-location -r requirements.txt
	${ENV}/bin/python setup.py --quiet install
	pwd > ${ENV}/lib/${PYTHON}/site-packages/perfrunner.pth

CONTAINER_PASSWORD := puppet
docker-compose:
	docker-compose up -d --build perfrunner
	docker exec -it perfrunner make
	docker exec -it perfrunner sh -c "echo 'root:${CONTAINER_PASSWORD}' | chpasswd"
	docker exec -it perfrunner /bin/bash
