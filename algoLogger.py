
# encoding: UTF-8

# This is for the log functionality. This class is referred to algo.strategy.StrategyLog.


import logging, logging.config, logging.handlers


class AlgoLogger:

    """
    This class is an encapsulation for the system logging class

    """
    def __init__(self, log_conf_file_name, logger_name):
        logging.config.fileConfig(log_conf_file_name)
        self.__logger = logging.getLogger(logger_name)

    def debug(self, log_msg, *args, **kwargs):
        self.__logger.disabled = False
        self.__logger.debug(log_msg,*args, **kwargs)

    def info(self, log_msg, *args, **kwargs):
        self.__logger.disabled = False
        self.__logger.info(log_msg, *args, **kwargs)

    def warning(self, log_msg, *args, **kwargs):
        self.__logger.disabled = False
        self.__logger.warning(log_msg, *args, **kwargs)

    def error(self, log_msg, *args, **kwargs):
        self.__logger.disabled = False
        self.__logger.error(log_msg, *args, **kwargs)

    @staticmethod
    def get_logger(logger_name):
        return AlgoLogger("logging.conf", logger_name)

algoLogger = AlgoLogger("logging.conf", "algo")