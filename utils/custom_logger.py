import logging
from logging.config import dictConfig
import yaml


class CustomLogger(object):
    def __init__(self, file, name='root'):
        self.file = file
        self.name = name
        self.logger = self._configure_logger()
        self.logger.info('Logger configured')

    def _configure_logger(self):
        with open(self.file, 'r') as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
            logging.config.dictConfig(config)
        return logging.getLogger() if self.name=='root' else logging.getLogger(self.name)
