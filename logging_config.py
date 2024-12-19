# logging_config.py
import logging


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Log to stdout
    )
    return logging.getLogger(__name__)


# dev version
# import logging
#
#
# def configure_logging():
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         filename="app.log",  # Log to a file named 'app.log'
#         filemode="a",  # Append to the log file
#     )
#     return logging.getLogger(__name__)
