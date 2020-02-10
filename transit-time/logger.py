import logging
import os

import google.cloud.logging


def setup():
    env = os.environ.get("ENV")
    if env == "prod" or env == "GCP_RUN" or "GAE_INSTANCE" in os.environ:
        client = google.cloud.logging.Client()
        client.setup_logging()
    else:
        logging.basicConfig(
            # send INFO and above to stderr
            level=os.environ.get("LOGLEVEL", "INFO"),
            # format with timestamps
            format="[%(asctime)s][%(levelname)s] (%(name)s) %(message)s",
        )
