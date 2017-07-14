from seriesly import Seriesly

from logger import logger
from perfrunner.settings import StatsSettings


def main():
    s = Seriesly(StatsSettings.SERIESLY)

    dbs = s.list_dbs()
    for i, db in enumerate(reversed(dbs), start=1):
        logger.info('Compacting {} ({} / {})'.format(db, i, len(dbs)))
        result = s[db].compact()
        logger.info('Compaction finished: {}'.format(result))


if __name__ == '__main__':
    main()
