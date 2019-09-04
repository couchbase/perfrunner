Deploying with docker-compose 
---
    
**_Note:_** this isn't recommended for production deployments. However, this is useful for development where you don't wish to install the client dependencies on your machine, or have an incompatible OS.

Running perfrunner with docker-compose allows the perfrunner repo to be mounted as a volume, so live changes you make inside the repo, on your host machine, will be reflected in the container. 
Anything outside the repo (for example, system packages), will not be mirrored between container and host. This gives you a self-contained development environment.

#### Requirements ####

* [docker](https://docs.docker.com/install/)
* [docker-compose](https://docs.docker.com/compose/install/)

Ensure your cluster spec file has `localhost` or `127.0.0.1` specified as the client host:

    [clients]
    hosts =
        localhost
        
#### Setup #### 

To build, install perfrunner requirements, and gain a shell in the container:
    
    make docker-compose CONTAINER_PASSWORD=${password}
    
Where ${password} is the same as that supplied in the credentials section of your spec file.

When in the container, you can run commands as normal, eg:

    env/bin/perfrunner -c clusters/vagrant.spec -t tests/comp_bucket_20M.test

Simply disconnect with `exit` when done.


**Other useful commands include:**

* Exec back into container:
        
        docker exec -it perfrunner /bin/bash
        
* Kill container:
        
        docker kill perfrunner