.PHONY: build

build:
	virtualenv -p python2.7 env
	./env/bin/pip install --upgrade --quiet pip wheel
	./env/bin/pip install --quiet --find-links wheels -r requirements.txt
	./env/bin/python setup.py install
	pwd > env/lib/python2.7/site-packages/perfrunner.pth

clean:
	rm -fr build perfrunner.egg-info
	rm -f `find . -name *.pyc`

pep8:
	./env/bin/flake8 --statistics cbagent perfrunner spring
	./env/bin/isort --quiet --balanced --check-only --recursive perfrunner

nose:
	./env/bin/nosetests -v unittests.py

test: nose pep8
