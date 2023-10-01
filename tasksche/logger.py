import logging
import sys


def _get_logger(name: str, print_level=logging.DEBUG):
    formatter = logging.Formatter(
        fmt="%(levelname)5s "
            "[%(filename)15s:%(lineno)-4d %(asctime)s]"
            " %(message)s",
        datefmt='%H:%M:%S',
    )
    formatter_file = logging.Formatter(
        fmt="%(levelname)5s "
            "[%(filename)15s:%(lineno)-5d %(asctime)s]"
            " %(message)s",
        datefmt='%H:%M:%S',
    )
    logger_obj = logging.getLogger(name)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(print_level)
    file_handler = logging.FileHandler(
        '/tmp/task_sche.log',
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter_file)
    logger_obj.addHandler(stream_handler)
    logger_obj.addHandler(file_handler)
    logger_obj.setLevel(logging.DEBUG)
    return logger_obj


class Logger:
    logger = None

    def __new__(cls):
        if cls.logger is None:
            cls.logger = _get_logger('runner', print_level=logging.WARNING)
        return cls.logger


if __name__ == '__main__':
    singleton1 = Logger()
    print(singleton1)
    singleton1.info("hello world")

    sg2 = Logger()
    sg2.info('lg2')

    print(id(singleton1), id(sg2))
