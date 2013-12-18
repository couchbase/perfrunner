# Get repsository
echo "cd /tmp; rm -fr perfrunner; git clone git://github.com/couchbaselabs/perfrunner.git" | ssh root@node1 /bin/bash
echo "cd /tmp; rm -fr perfrunner; git clone git://github.com/couchbaselabs/perfrunner.git" | ssh root@node2 /bin/bash
echo "cd /tmp; rm -fr perfrunner; git clone git://github.com/couchbaselabs/perfrunner.git" | ssh root@node3 /bin/bash
echo "cd /tmp; rm -fr perfrunner; git clone git://github.com/couchbaselabs/perfrunner.git" | ssh root@node4 /bin/bash

# UNINSTALL
echo "cd /tmp/perfrunner/scripts/mongo; ./uninstall-mongo-local.sh" | ssh root@node1 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./uninstall-mongo-local.sh" | ssh root@node2 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./uninstall-mongo-local.sh" | ssh root@node3 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./uninstall-mongo-local.sh" | ssh root@node4 /bin/bash

# INSTALL
echo "cd /tmp/perfrunner/scripts/mongo; ./install-mongo-local.sh" | ssh root@node1 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./install-mongo-local.sh" | ssh root@node2 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./install-mongo-local.sh" | ssh root@node3 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./install-mongo-local.sh" | ssh root@node4 /bin/bash

# Start mongos
echo "cd /tmp/perfrunner/scripts/mongo; ./start-mongos-4.sh" | ssh root@node1 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./start-mongos-4.sh" | ssh root@node2 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./start-mongos-4.sh" | ssh root@node3 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./start-mongos-4.sh" | ssh root@node4 /bin/bash

# REPLICA
echo "cd /tmp/perfrunner/scripts/mongo; ./init-replica-4.sh" | ssh root@node1 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-replica-4.sh" | ssh root@node2 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-replica-4.sh" | ssh root@node3 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-replica-4.sh" | ssh root@node4 /bin/bash

echo "sleeping 10 minutes"

python -c "import time; time.sleep(10 * 60);"

# SHARDING
echo "cd /tmp/perfrunner/scripts/mongo; ./init-sharding-4.sh" | ssh root@node1 /bin/bash

# INDEX
echo "cd /tmp/perfrunner/scripts/mongo; ./init-index.sh" | ssh root@node1 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-index.sh" | ssh root@node2 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-index.sh" | ssh root@node3 /bin/bash
echo "cd /tmp/perfrunner/scripts/mongo; ./init-index.sh" | ssh root@node4 /bin/bash

echo "sleeping - 2 minutes"

python -c "import time; time.sleep(2 * 60);"
