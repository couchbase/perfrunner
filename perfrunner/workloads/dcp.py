import json
import shlex
from typing import Any, Optional

from logger import logger
from perfrunner.helpers.local import run_java_dcp_client
from perfrunner.helpers.misc import run_local_shell_command
from perfrunner.settings import DCPDrainSettings, PhaseSettings, TargetSettings


def java_dcp_client(workload_settings: PhaseSettings,
                    target: TargetSettings,
                    timer: int,
                    instance: int):

    connection_string = target.connection_string
    messages = workload_settings.items
    config_file = workload_settings.java_dcp_config
    collections = None

    if workload_settings.collections is not None:
        access_targets = []
        num_load_targets = 0
        target_scope_collections = workload_settings.collections[target.bucket]
        for scope in target_scope_collections.keys():
            for collection in target_scope_collections[scope].keys():
                if target_scope_collections[scope][collection]['load'] == 1:
                    num_load_targets += 1
                    if target_scope_collections[scope][collection]['access'] == 1:
                        access_targets += [scope+":"+collection]

        if workload_settings.java_dcp_stream == "all":
            messages = (workload_settings.items // num_load_targets) * len(access_targets)
            collections = access_targets

        elif workload_settings.java_dcp_stream == "even_split":
            split_access_targets = [access_targets[seq_id]
                                    for seq_id
                                    in range(instance,
                                             len(access_targets),
                                             workload_settings.java_dcp_clients)]
            messages = (workload_settings.items // num_load_targets) * len(split_access_targets)
            collections = split_access_targets

    run_java_dcp_client(connection_string=connection_string,
                        messages=messages,
                        config_file=config_file,
                        instance=instance,
                        collections=collections)


def run_dcpdrain(workload_settings: DCPDrainSettings,
                 target: TargetSettings,
                 timer: Optional[int],
                 instance: int) -> dict[str, Any]:
    """
    Run dcpdrain on the remote server for the given target.

    workload_settings is expected to have dcpdrain-related attrs:
      - binary_path (path to dcpdrain on server)
      - num_connections (optional)
      - buffer_size (optional)
      - acknowledge_ratio (optional)
      - extra_args (optional)
      - name (optional)

    target is a TargetSettings instance (should have node and bucket)
    timer, instance follow the usual signature expected by WorkloadPhase.
    """
    # Compute remote host and bucket from target
    host = getattr(target, "node", None)
    bucket = getattr(target, "bucket", None)
    if not host or not bucket:
        raise RuntimeError("run_dcpdrain: target missing host or bucket")

    # read settings
    binary = getattr(workload_settings, "binary_path", None)
    if not binary:
        raise RuntimeError("run_dcpdrain: dcpdrain binary_path not set in workload_settings")

    num_connections = workload_settings.num_connections
    buffer_size = workload_settings.buffer_size
    ack_ratio = workload_settings.acknowledge_ratio
    name = workload_settings.name
    extra_args = workload_settings.extra_args

    # derive REST creds
    rest_username, rest_password = workload_settings.rest_creds

    # prepare TLS option
    tls_arg = None
    # 1) explicit triple: workload_settings.tls_opts if present
    if workload_settings.tls_opts is not None:
        tls_arg = f"--tls={workload_settings.tls_opts}"
    # 2) boolean use_tls (no cert/key)
    elif workload_settings.use_tls:
        tls_arg = "--tls"

    # Build the dcpdrain command
    cmd = [binary,
           "--host", f"{host}:" + ("11210" if not tls_arg else "11207"),
           "--ipv4",
           "--user", rest_username,
           "--password", rest_password,
           "--bucket", str(bucket)]

    # Add tls arg if present
    if tls_arg:
        cmd += [tls_arg]

    cmd += ["--num-connections", str(num_connections)]
    cmd += ["--buffer-size", str(buffer_size)]
    cmd += ["--acknowledge-ratio", str(ack_ratio)]
    if name:
        cmd += ["--name", name]

    # request JSON summary
    cmd += ["--json"]

    if extra_args:
        cmd += shlex.split(extra_args)

    cmd_str = " ".join(shlex.quote(p) for p in cmd)

    logger.info("run_dcpdrain: running on %s: %s", host, cmd_str)
    stdout, stderr, rc = run_local_shell_command(cmd_str)

    # try parsing last JSON line from stdout
    summary = None
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            summary = json.loads(line)
            break
        except Exception:
            continue

    # Log and return structured info for celery result
    result = {
        "host": host,
        "rc": rc,
        "summary": summary,
        "stdout_tail": "\n".join(stdout.splitlines()[-20:]),
        "stderr_tail": "\n".join(stderr.splitlines()[-20:])
    }

    if rc != 0:
        logger.warning(
                "run_dcpdrain: failed on %s rc=%s stderr=%s",
                host,
                rc,
                result["stderr_tail"],
        )
    else:
        logger.info(
                "run_dcpdrain: completed on %s rc=%s summary=%s",
                host,
                rc,
                summary,
        )

    # Return result so Celery can store / return it (if invoked via a task)
    return result

