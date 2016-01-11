build: ; \
    virtualenv -p python2.7 env; \
    ./env/bin/pip install --download-cache /tmp/pip -r requirements.txt

clean: ; \
    rm -fr env; \
    rm -f `find . -name *.pyc`

flake8: ; \
    ./env/bin/flake8 --ignore=E501,E731 --exclude perfrunner/workloads/revAB/fittingCode perfrunner

nose: ; \
    ./env/bin/nosetests -v unittests.py

test: nose flake8;
