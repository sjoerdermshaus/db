import logging
from logging.config import dictConfig
import yaml


class CustomLogger(object):
    def __init__(self, file):
        self._file = file
        self.logger = self._configure_logger(self._file)
        self.logger.info('Logger configured')

    @staticmethod
    def _configure_logger(file):
        with open(file, 'r') as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
            dictConfig(config)
        return logging.getLogger(__name__)
