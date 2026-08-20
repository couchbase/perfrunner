from perfrunner.helpers.local import (
    download_dotnet_client,
    get_java_classpath,
    run_custom_cmd,
)
from perfrunner.settings import PhaseSettings, TargetSettings

"""This workload covers a collection of micro benchmarks which are used for
testing individual aspects of language SDKs.

See: `sdks/README.md`"""

RUN_COMMANDS = {
    'libc': 'LCB_LOGLEVEL=5 ./{benchmark_name}',
    'java': 'java -cp {benchmark_name}/target/{benchmark_name}.jar:{classpath} '
            'com.couchbase.qe.perf.Main',
    'python': 'PYCBC_LOG_LEVEL=trace ../../env/bin/python {benchmark_name}.py',
    'go': 'go run .',
    'dotnet': 'dotnet run --project {benchmark_name}'
}

RUN_PARAMS = '--host {host} ' \
    '--bucket {bucket} ' \
    '--username {username} ' \
    '--password {password} ' \
    '--num-items {num_items} ' \
    '--num-threads {num_threads} ' \
    '--timeout {timeout} ' \
    '--keys {keys} ' \
    '--time {time} ' \
    '--c-poll-interval {config_poll_interval} ' \
    '>{benchmark_name}_stdout.log 2>../../{benchmark_name}_stderr.log'


def sdks_benchmark_workload(workload_settings: PhaseSettings, target: TargetSettings, *args):
    """Start an  run a workload using the benchmark specified in the workload settings."""
    if workload_settings.sdk_type in ['libc']:
        timeout = workload_settings.sdk_timeout * 1000  # to microseconds
        config_poll_interval = workload_settings.config_poll_interval * 1000  # to microseconds
    else:
        timeout = workload_settings.sdk_timeout  # ms
        config_poll_interval = workload_settings.config_poll_interval  # ms

    params = RUN_PARAMS.format(benchmark_name=workload_settings.benchmark_name,
                           host=target.node,
                           bucket=target.bucket,
                           username=target.username,
                           password=target.password,
                           num_items=workload_settings.items,
                           num_threads=workload_settings.workers,
                           timeout=timeout,
                           keys=workload_settings.keys,
                           time=workload_settings.time,
                           config_poll_interval=config_poll_interval)
    if workload_settings.sdk_type == 'java':  # include mvn generated classpath
        classpath = get_java_classpath(workload_settings.benchmark_name)
        run_cmd = RUN_COMMANDS.get(workload_settings.sdk_type).format(
            benchmark_name=workload_settings.benchmark_name,
            classpath=classpath)
    else:
        run_cmd = RUN_COMMANDS.get(workload_settings.sdk_type).format(
            benchmark_name=workload_settings.benchmark_name)

    cmd = f"{run_cmd} {params}"
    run_custom_cmd(f"sdks/{workload_settings.sdk_type}/", cmd, "")


def get_sdk_build_command(benchmark_name: str, sdk_type: str, version: str = None) -> str:
    """Return a build command for a specific SDK type.

    If the specified SDK is not supported or compiled, `None` is returned
    """
    cmd = None
    if sdk_type == 'libc':
        cmd = f"cc -o {benchmark_name} {benchmark_name}.c -lcouchbase -lpthread -lm"
    elif sdk_type == 'java':
        cmd = (
            f"cd {benchmark_name} && mvn package dependency:build-classpath "
            "-Dmdep.outputFilterFile=true -Dmdep.outputFile=cp.txt "
            f'"-Dcouchbase3.version={version}"'
        )
    elif sdk_type == 'go':
        cmd = (
            f"go mod init github.com/couchbase/perfrunner/{benchmark_name} && "
            f"go get github.com/couchbase/gocb/v2@{version} && "
            "go mod tidy && go build ."
        )
    elif sdk_type == 'dotnet':
        if 'commit:' in version:
            source = download_dotnet_client(version.replace('commit:', ''))
            cmd = (
                f"dotnet add {benchmark_name} package CouchbaseNetClient "
                f"-s {source}/src/Couchbase/bin/Release --prerelease && "
                f"dotnet build {benchmark_name}"
            )
        else:
            cmd = (
                f"dotnet add {benchmark_name} package CouchbaseNetClient --version {version} && "
                f"dotnet build {benchmark_name}"
            )

    return cmd
