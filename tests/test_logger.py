import sys
import logging
import logging.handlers
from unittest.mock import Mock

from ebi_eva_common_pyutils.logger import AppLogger, LoggingConfiguration
from tests.test_common import TestCommon


class TestLoggingConfiguration(TestCommon):

    def setUp(self) -> None:
        self.log_cfg = LoggingConfiguration()
        self.log_cfg.reset()

    def test_formatters(self):
        default = self.log_cfg.default_formatter
        assert default.datefmt == '%Y-%b-%d %H:%M:%S'
        assert default._fmt == '[%(asctime)s][%(name)s][%(levelname)s] %(message)s'

        blank = self.log_cfg.blank_formatter
        assert blank.datefmt is None
        assert blank._fmt == '%(message)s'

        assert self.log_cfg.formatter is default

        assert self.log_cfg.handlers == set()
        assert self.log_cfg._log_level == logging.INFO

    def test_get_logger(self):
        l = self.log_cfg.get_logger('a_logger')
        assert l.level == self.log_cfg._log_level
        assert l in self.log_cfg.loggers.values()
        assert list(self.log_cfg.handlers) == l.handlers

    def test_add_handler(self):
        h = logging.StreamHandler(stream=sys.stdout)
        self.log_cfg.add_handler(h)

        assert h in self.log_cfg.handlers
        assert h.formatter is self.log_cfg.formatter
        assert h.level == logging.INFO

    def test_set_log_level(self):
        h = logging.StreamHandler(stream=sys.stdout)
        self.log_cfg.add_handler(h, level=logging.INFO)
        l = self.log_cfg.get_logger('a_logger')
        assert h.level == l.level == logging.INFO

        self.log_cfg.set_log_level(logging.DEBUG)
        assert h.level == l.level == logging.DEBUG

    def test_set_formatter(self):
        h = logging.StreamHandler(stream=sys.stdout)
        self.log_cfg.add_handler(h)

        self.log_cfg.set_formatter(self.log_cfg.blank_formatter)
        assert h.formatter is self.log_cfg.blank_formatter
        self.log_cfg.set_formatter(self.log_cfg.default_formatter)
        assert h.formatter is self.log_cfg.default_formatter

    def test_reset(self):
        l = self.log_cfg.get_logger('a_logger')
        assert not l.handlers  # not set up yet

        self.log_cfg.add_stdout_handler()
        assert len(self.log_cfg.handlers) == 1
        assert len(l.handlers) == 1
        self.log_cfg.reset()

        assert not self.log_cfg.handlers
        assert not l.handlers  # logger still exists, has no handlers


class TestAppLogger(TestCommon):
    def setUp(self):
        self.log_cfg = LoggingConfiguration()
        self.app_logger = AppLogger()
        self.app_logger.log_cfg = self.log_cfg

    def tearDown(self):
        self.app_logger = None
        self.log_cfg = None

    def test_info(self):
        self.log_cfg.get_logger = Mock()
        self.app_logger.info('Things')
        self.app_logger._logger.info.assert_called_with('Things')
        self.log_cfg.get_logger.assert_called_with('AppLogger')
