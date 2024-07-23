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

    cert = SSLCertificate(args.addresses.split(","))

    path = os.path.abspath(args.dest or cert.INBOX)
    if not os.path.exists(path):
        os.makedirs(path)
    cert.INBOX = path
    cert.generate()
    logger.info(f"Generated certificates located at {path}")


if __name__ == "__main__":
    main()
