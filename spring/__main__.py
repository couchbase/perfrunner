import json
from argparse import ArgumentParser
from multiprocessing import set_start_method
from urllib.parse import urlparse

from perfrunner.settings import TargetSettings, TestConfig
from spring.wgen3 import WorkloadGen

set_start_method("fork")

PROG = "spring"
USAGE = "%(prog)s -t <test file> [OPTIONS]"


def get_args():
    parser = ArgumentParser(prog=PROG, usage=USAGE)
    parser.add_argument(
        "--uri",
        default="cb://Administrator:password@127.0.0.1/default",
        help="connection URI in the form of: cb://[user:pass]@host:[port]/<bucket>",
    )
    parser.add_argument(
        "-t",
        "--test",
        required=True,
        help="path to a test configuration file",
    )
    parser.add_argument(
        "-f",
        "--prefix",
        default="",
        help="id prefix",
    )
    parser.add_argument(
        "-p",
        "--phase",
        default="access",
        help="phase to run. Example: load, access",
    )
    parser.add_argument(
        "-c",
        "--cloud",
        default="{}",
        help="cloud target settings such as 'cluster_svc' as a json string",
    )
    parser.add_argument("override", nargs="*", help="custom test settings")
    return parser.parse_args()


def main():
    args = get_args()
    test_config = TestConfig()
    test_config.parse(args.test, args.override)

    phase = f"{args.phase}_settings"
    if not hasattr(test_config, phase):
        print(f"Unknown phase settings: {phase}")
        exit(1)

    phase_settings = getattr(test_config, phase)
    params = urlparse(args.uri)
    if not params.hostname or not params.path:
        print("Invalid connection URI")
        exit(1)

    target_settings = TargetSettings(
        host=params.hostname,
        bucket=params.path[1:],
        username=params.username or "Administrator",
        password=params.password or "password",
        prefix=args.prefix,
        cloud=json.loads(args.cloud),
    )
    wg = WorkloadGen(phase_settings, target_settings)
    wg.run()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
