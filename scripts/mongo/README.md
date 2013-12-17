# MongoDB configurations

# Cluster setup

The scripts will setup a 4-node mongodb cluster with swirl replication pattern.

Swirl replication pattern reflects a replica=1 couchbase cluster configuration.

# Limitations

- Nodes are named as node1, node2, node3, node4. Put the DNS items into /etc/hosts.

- Public key auth needed for client to fire commands through remote SSH session.

- Be sure to setup your firewall correctly, 'iptable -F' it is.

# Usage

Run setup.sh from you client (jenkins), you are good to go.
