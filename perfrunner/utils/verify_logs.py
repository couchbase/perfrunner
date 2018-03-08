import glob
import zipfile

from logger import logger

GOLANG_LOG_FILES = ("eventing.log",
                    "fts.log",
                    "goxdcr.log",
                    "indexer.log",
                    "projector.log",
                    "query.log")


def check_for_golang_panic(file_name: str):
    zf = zipfile.ZipFile(file_name)
    panic_files = []
    for name in zf.namelist():
        if any(log_file in name for log_file in GOLANG_LOG_FILES):
            data = zf.read(name)
            if "panic" in str(data):
                panic_files.append(name)
    return panic_files


def validate_logs(file_name: str):
    return check_for_golang_panic(file_name)


def main():
    panics = {}

    for filename in glob.iglob('./*.zip'):
        panic_files = validate_logs(filename)
        if panic_files:
            panics[filename] = panic_files

    if panics:
        logger.interrupt("Following panics found: {}".format(panics))


if __name__ == '__main__':
    main()
