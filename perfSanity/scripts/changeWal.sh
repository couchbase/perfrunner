curl -u Administrator:password 10.17.0.106:9102/settings |  python -m json.tool | sed 's/"indexer.settings.wal_size": 4096,/"indexer.settings.wal_size": 81920,/' > /tmp/walchange
curl -u Administrator:password 10.17.0.106:9102/settings -d @/tmp/walchange
