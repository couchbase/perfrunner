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

clean:
	rm -fr build perfrunner.egg-info dist dcptest kvgen cbindexperf YCSB
	find . -name '*.pyc' -o -name '*.pyo' | xargs rm -f

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

vendor-sync: go-tools
	${GOVENDOR} sync

go-tools:
	go get -u github.com/kardianos/govendor

ycsb:
	git clone git://github.com/brianfrankcooper/YCSB.git
	cd YCSB && mvn -pl com.yahoo.ycsb:couchbase2-binding -am -Dcheckstyle.skip -DskipTests clean package
