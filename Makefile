build:
	virtualenv -p python2.7 env
	./env/bin/pip install -r requirements.txt

clean:
	rm -fr env
	rm -f `find . -name *.pyc`

pep8:
	./env/bin/flake8 --statistics perfrunner
	./env/bin/isort --balanced --check-only --recursive --verbose perfrunner

nose:
	./env/bin/nosetests -v unittests.py

test: nose pep8
