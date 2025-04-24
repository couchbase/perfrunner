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

[storage]
data = /data

[parameters]
OS = {{os_info}}
CPU = {{cpu_info}}
Memory = {{mem_quota}} GB
Disk = {{storage_info}}
