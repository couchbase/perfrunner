from logger import logger
from seriesly import Seriesly

from perfrunner.settings import StatsSettings


def main():
    s = Seriesly(StatsSettings.SERIESLY['host'])
    for db in s.list_dbs():
        logger.info('Compacting {}'.format(db))
        result = s[db].compact()
        logger.info('Compaction finished: {}'.format(result))


if __name__ == '__main__':
    main()
