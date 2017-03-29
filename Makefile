.PHONY: build

SHELL := /bin/bash

PYTHON := python3.5
ENV := env
GOVENDOR := ${GOPATH}/bin/govendor

build:
	virtualenv --quiet --python ${PYTHON} ${ENV}
	${ENV}/bin/pip install --upgrade --quiet pip wheel
	${ENV}/bin/pip install --quiet -r requirements.txt
	${ENV}/bin/python setup.py --quiet install
	pwd > ${ENV}/lib/${PYTHON}/site-packages/perfrunner.pth

clean:
	rm -fr build perfrunner.egg-info dist dcptest kvgen cbindexperf rachell *.db *.log
	find . -name '*.pyc' -o -name '*.pyo' -o -name __pycache__ | xargs rm -fr

pep8:
	${ENV}/bin/flake8 --statistics cbagent perfdaily perfrunner scripts spring
	${ENV}/bin/isort --quiet --check-only --recursive cbagent perfdaily perfrunner scripts spring

nose:
	${ENV}/bin/nosetests -v unittests.py

gofmt:
	gofmt -e -d -s go && ! gofmt -l go | read

test: nose pep8 gofmt

dcptest: vendor-sync
	go build ./go/dcptest

cbindexperf: buildquery
	go build ./go/cbindexperf

buildquery: vendor-sync
	cd vendor/github.com/couchbase/query/parser/n1ql && sh build.sh

kvgen: vendor-sync
	go build ./go/kvgen

rachell:
	go build ./go/rachell

vendor-sync: go-tools
	${GOVENDOR} sync

go-tools:
	go get -u github.com/kardianos/govendor

memblock:
	gcc -o memblock c/memblock/memblock.c
