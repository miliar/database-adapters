import logging
import traceback as tb
from abc import ABC, abstractmethod
from collections import namedtuple


class AdapterAbstract(ABC):
    def __init__(self):
        super().__init__()
        self.__logger = self.__get_logger()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.__handle_exception(*exception)

    def __get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def __handle_exception(self, exc_type, exc_value, traceback):
        if traceback:
            exception = tb.format_exception(exc_type, exc_value, traceback)
            self.__logger.error(exception)

    def log_exception(self, exc):
        self.__logger.error(exc)

    @abstractmethod
    def get_result_table(self, *args):
        """ Should return a Table instance"""

    @abstractmethod
    def create_table(self, table, *args):
        """ Should create a table from a Table instance """

    @abstractmethod
    def delete_table(self, *args):
        """Should delete a table"""


Table = namedtuple('Table', ['schema', 'row_iter'])
