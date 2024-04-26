# Generating Perfrunner Docker Images

## Deploying with docker-compose
**_Note:_** this isn't recommended for production deployments. However, this is useful for development where you don't wish to install the client dependencies on your machine, or have an incompatible OS.

Running perfrunner with docker-compose allows the perfrunner repo to be mounted as a volume, so live changes you make inside the repo, on your host machine, will be reflected in the container. 
Anything outside the repo (for example, system packages), will not be mirrored between container and host. This gives you a self-contained development environment.

#### Requirements ####

* Recent version of [docker](https://docs.docker.com/install/)
* Docker compose should be integrated, if not, check [compose migration](https://docs.docker.com/compose/migrate/)

#### Setup ####
First create a local testing network. This provides isolation and you only have to do it once:

    docker network create local-perf-testing

Keep the name the same, perfrunner docker compose is setup to find a network with this name. Then, to build, install perfrunner requirements, and gain a shell in the container:

    make docker-compose CONTAINER_PASSWORD=${password}

Where ${password} is the same as that supplied in the credentials section of your spec file.

Simply disconnect with `exit` when done.

**Other useful commands include:**

* Exec back into the container:

        docker exec -it perfrunner /bin/bash

* Kill the perfrunner container:

        docker kill perfrunner

* Restart the perfrunner container:

        docker start perfrunner

### Installing and Connecting to Couchbase Server

#### Installing Couchbase server using Docker

Follow the instructions from the [documentations](https://docs.couchbase.com/server/current/install/getting-started-docker.html) on how to install a couchbase server locally using docker. Remember to add ` --network local-perf-testing` in your docker run command when starting the couchbase server. 

#### Preparing a local docker cluster spec file
To run a perfrunner test in a container, you can run commands as normal, eg:

    env/bin/perfrunner -c clusters/cluster_spec.spec -t tests/comp_bucket_20M.test

**_Note:_** Make sure the cluster specified by `-c` is free and there are no any running jobs at the moment using the cluster.

To prepare a local cluster spec for testing, copy any example cluster available in `clusters/` directory and name it say `clusters/local.spec`. Modify the following two sections, `clusters` and `clients` as in the example below:

    [clusters]
    local =
        <server-ip>:kv

    [clients]
    hosts =
        localhost

The client hosts should be `localhost` or `127.0.0.1`. The server IP can be obtained by inspecting your Couchbase server docker container and look for IPAddress. For instance, if my server container name is `couchbase`:

        docker inspect couchbase | grep IPAddress -

## Generating docker images for Kubernetes workers
To generate docker images available at [docker hub](https://hub.docker.com/r/perflab/perfrunner),  run the following commands:
* Make sure the image is built for `linux/amd64`. This is what we use for our client machines. You can skip this step if running on `linux/amd64` platform or building for local testing.

        export DOCKER_DEFAULT_PLATFORM=linux/amd64

* Optionaly also specify `PYTHON_VERSIONS` and `PYENV_VERSION` build arguments (with `--build-arg`) to specify which Python versions and Pyenv version to install in the image.
* Build perfrunner image:

        docker build --target perfrunner docker/

* Tag the image appropriately

        docker tag <generated-image-tag> perflab/perfrunner

    The generated image tag will be displaed at the end of the previous build step.

This will tag the new image as `@latest`. You can use your personal hub to build experimental builds. Or simply:

        make docker