.PHONY: build

PYTHON := python2.7
ENV := env
GOVENDOR := ${GOPATH}/bin/govendor

build:
	virtualenv -p ${PYTHON} ${ENV}
	${ENV}/bin/pip install --upgrade --quiet pip wheel
	${ENV}/bin/pip install --quiet -r requirements.txt
	${ENV}/bin/python setup.py install
	pwd > ${ENV}/lib/${PYTHON}/site-packages/perfrunner.pth

clean: clean-dcptest
	rm -fr build perfrunner.egg-info dist
	find . -name '*.pyc' -o -name '*.pyo' | xargs rm -f

pep8:
	${ENV}/bin/flake8 --statistics cbagent perfdaily perfrunner scripts spring
	${ENV}/bin/isort --quiet --check-only --recursive cbagent perfdaily perfrunner scripts spring

nose:
	${ENV}/bin/nosetests -v unittests.py

test: nose pep8

dcptest: vendor-sync
	go build ./go/dcptest

kvgen: vendor-sync
	go build ./go/kvgen

vendor-sync: go-tools
	${GOVENDOR} sync

go-tools:
	go get -u github.com/kardianos/govendor

clean-dcptest:
	rm -f dcptest
