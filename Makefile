build:
	virtualenv -p python2.7 env
	./env/bin/pip install --upgrade --quiet pip wheel
	./env/bin/pip install --quiet --find-links wheels -r requirements.txt

clean:
	rm -fr env
	rm -f `find . -name *.pyc`

pep8:
	./env/bin/flake8 --statistics perfrunner
	./env/bin/isort --quiet --balanced --check-only --recursive perfrunner

nose:
	./env/bin/nosetests -v unittests.py

test: nose pep8
