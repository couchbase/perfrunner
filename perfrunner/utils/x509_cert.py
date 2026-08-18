import os
from argparse import ArgumentParser

from logger import logger
from perfrunner.helpers.misc import SSLCertificate


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "-a", "--addresses", default="localhost,127.0.0.1", help="comma separated list of addresses"
    )
    parser.add_argument(
        "-d", "--dest", help="destination directory path for generated certificates"
    )

    args = parser.parse_args()

    dest = args.dest or SSLCertificate.INBOX
    SSLCertificate(args.addresses.split(","), output_dir=dest).generate_server_cert()

    logger.info(f"Generated certificates located at {os.path.abspath(dest)}")


if __name__ == "__main__":
    main()
