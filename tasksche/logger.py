import datetime
import locale
import logging
import sys
from loguru import logger


class DeltaTimeFormatter(logging.Formatter):
    def format(self, record):
        duration = datetime.datetime.utcfromtimestamp(
            record.relativeCreated / 1000
        )
        record.delta = (
            "+"
            + duration.strftime("%H:%M:%S")
            + f".{duration.microsecond // 1000:03d}"
        )
        return super().format(record)


def _get_logger(name: str, print_level=logging.DEBUG):
    return logger
    # formatter = DeltaTimeFormatter(
    #     fmt="%(name)25s %(levelname)5s "
    #     "[%(filename)15s:%(lineno)-4d %(delta)s]"
    #     " %(message)s",
    #     datefmt="%H:%M:%S",
    # )
    # formatter_file = logging.Formatter(
    #     fmt="%(levelname)5s "
    #     "[%(filename)15s:%(lineno)-5d %(asctime)s.%(msecs)03d]"
    #     " %(message)s",
    #     datefmt="%H:%M:%S",
    # )
    # logger_obj = logging.getLogger(name)

    # def check_handler_exists(logger, handler_type):
    #     for handler in logger.handlers:
    #         return True
    #     return False

    # # Example of checking if a StreamHandler exists in a logger
    # logger = logging.getLogger()
    # if check_handler_exists(logger, logging.StreamHandler):
    #     # print("StreamHandler exists in the logger")
    #     return logger
    # # else:
    # #     print("StreamHandler does not exist in the logger")
    # stream_handler = logging.StreamHandler(sys.stderr)
    # stream_handler.setFormatter(formatter)
    # stream_handler.setLevel(print_level)
    # file_handler = logging.FileHandler(
    #     "/tmp/task_sche.log",
    # )
    # file_handler.setLevel(logging.DEBUG)
    # file_handler.setFormatter(formatter_file)
    # logger_obj.addHandler(stream_handler)
    # logger_obj.addHandler(file_handler)
    # logger_obj.setLevel(logging.DEBUG)
    # return logger_obj


# class _Logger:
#     logger = None

#     def __new__(cls) -> logging.Logger:
#         if cls.logger is None:
#             cls.logger = _get_logger("runner", print_level=logging.DEBUG)
#         return cls.logger


_logger = None


def Logger():
    global _logger
    if _logger is None:
        _logger = _get_logger("runner", print_level=logging.DEBUG)
    return _logger


if __name__ == "__main__":
    singleton1 = Logger()
    print(singleton1)
    singleton1.info("hello world")

    sg2 = Logger()
    sg2.info("lg2")

    print(id(singleton1), id(sg2))
