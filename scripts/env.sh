#!/bin/bash -ex
virtualenv /tmp/prenv${nickname}
/tmp/prenv${nickname}/bin/pip install --upgrade -r requirements.txt
