[clusters]
aws =
    {% for server in servers -%}
        {{server}}:kv,index,n1ql
    {% endfor %}

[clients]
hosts =
    {% for client in clients -%}
        {{client}}
    {% endfor %}
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh =  root:couchbase
