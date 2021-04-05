#  Copyright (C) 2021
#  Author: Zubair Ahmed <zub41r.ahm3d@gmail.com>

from scraper.pipeline import Pipeline


# TODO: class Worker(multiprocessing.Process):
class Worker:
    """A worker runs a pipeline of processing stages as a process."""

    def __init__(self, logger, config, database_conn):
        """Every time a worker is initiated, there are a few items to setup."""

        # Keep a copy of the configuration
        self.logger = logger
        self.config = config
        self.database_conn = database_conn

        # Pass the worker configuration through to the pipeline
        self.pipeline = Pipeline(logger=self.logger, config=self.config, database_conn=self.database_conn)

        # TODO: Run multiprocessing setup as per parent.
        # multiprocessing.Process.__init__(self)

    def run(self):
        """Run method overrides Process.run() and lets the worker work."""

        # Show the pipeline to the user.
        self.pipeline.show()

        # Run the pipeline
        self.pipeline.run()
