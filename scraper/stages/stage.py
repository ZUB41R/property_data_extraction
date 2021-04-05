#  Copyright (C) 2021
#  Author: Zubair Ahmed <zub41r.ahm3d@gmail.com>

import sys
import traceback
import time
import random
from threading import Thread
import logging
from utils.helper_functions import set_logger


class Minion(Thread):
    """ A minion is a thread that does some work() and holds results."""

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        """Every time a minion is born, there are a few items to setup."""
        super().__init__(group=group, target=target, name=name, daemon=daemon)

        # A variable number of arguments can be passed to the minion using this method.
        self.suburb_task = args[0]
        self.kwargs = kwargs

        # The type of minion here is part name, and part the task its doing.
        minion_type = str(type(self)) + ":" + repr(self.suburb_task)

        # Its got a logger assigned to this.
        self.logger_minion = logging.getLevelName(minion_type)

        # A minion can succeed or fail or error.
        self.success = None

        # A minion starts off not errorred.
        self.errored = False

        # A minion starts of not done or handled.
        self.done = False
        self.handled = False

    def run(self):
        """ All minons can run() some work().

        There should be no need to override this function. Override work() instead.
        This method further ensures that error handling is done in a standardised way.
        Error handling is not required to be implemented by the dev defining stages or minion work.
        """

        try:

            # Success depends on the work() function
            self.success = self.work()

            # We can assert (but dont) that:
            # self.errored = False

        except Exception as error:

            # Exception information is compiled into a traceback string representation.
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_string = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))

            # print("(Handled) Error:", error_string)

            # .. and placed into the task itself.
            my_suburb_task = self.suburb_task
            my_suburb_task.error = error_string     # If we're in error, the task.error is the error stacktrace.

            # Before the minion sets its state accordingly.
            self.success = False
            self.errored = True

        finally:

            # Either way, make that we are done.
            self.done = True

    def work(self):
        """ This is the method that defines the minion. Override this.

        The work method below demonstrates an example of doing work that: takes time,
        results in success or failure and occasionally results in error.

        :return:
        """

        suburb_task = self.suburb_task
        suburb_task_string = repr(suburb_task)

        # Operations in this method must be thread-safe, use logger instead of print.
        self.logger_minion.debug("Working:" + suburb_task_string)

        # The work that this stage does is here:
        time.sleep(4)  # We use sleep to pretend something is happening.

        # Lets use a random integer to represent (hypothetical) work success
        random_state = random.randint(0,1000)

        # If a random number is less than 1: (0.1% chance of error)
        if random_state < 1:
            # Test some error.
            self.logger_minion.debug("Error/Failed:" + suburb_task_string)
            raise RuntimeError('Designed, example error. Override work() in Minion.')

        # else if its less than 2 and greater than 1: (0.1% chance of failure)
        elif random_state < 2:
            # Recognised as failed.
            self.logger_minion.debug("Failed:" + suburb_task_string)
            return False

        # otherwise: we finished fine.
        else:
            # We completed successfully.
            self.logger_minion.debug("Success:" + suburb_task_string)
            return True


class Stage(object):
    """ Stage takes a list of tasks from to-do and moves them to errored or done.

    To do this: a stage births minions to process a task and cleans up dead minions once done, failed or error-ed.
    Each minion is its own thread and are limited in the stage by batch_size.
    If you are implementing your own stage:
    1. (Compulsory) Override make_minion() to make your stage run your minion type.
    2. (Optional) Override handle_failed if you want task that have success = False do to something else. eg. retry
    3. (Optional) Override start() and end() if you want to do something custom before launching minions.
    4. (Alternatively) Override run() if you don't want to use minions or want to use them differently.
    """

    def __init__(self, name):
        """Every time we create a stage, we need to set up some stuff."""

        # Every stage has a name (for logging and debugging)
        self.name = name

        # TODO: Implement locking?
        # self.todolock = threading.Lock()

        # There are empty lists for the Tasks to-do, done and in error.
        self.todo = []
        # print(f"self.todo: {self.todo}")
        # print(f"self.todo: {self.todo[0].__dict__}")
        self.done = []
        self.error = []

        # We keep track of the minions born to this stage.
        self.minions = []

        # We try to manage only 5 minions per stage, by default, but this can be
        # overridden.
        self.batch_size = 5

        # Logs generated from here are tagged with the stage name.
        self.logger_minion = logging.getLogger(self.name)

    def is_active(self):
        """ Checks if this stage is active (minions active or items in to-do."""

        # If there are any minions active, the stage is active.
        if len(self.minions) > 0:
            return True

        # If there are any items in the to-do list, the stage is active.
        if len(self.todo) > 0:
            return True

        if len(self.done) > 0:
            return True

        # Otherwise return False
        return False

    def handle_minions(self):
        """ Handle minions that have finished."""

        # Cleanup dead minions
        for minion in self.minions:

            if minion.done:

                if minion.success:

                    # If the task is done and a success, move it to [done].
                    self.done.append(minion.suburb_task)

                else:

                    if minion.errored:

                        # If the task is done but errored, move it to [errored].
                        self.error.append(minion.suburb_task)

                    else:
                        # Not an error. But not a success. Use the handle_failed method.
                        self.handle_failed(minion.suburb_task)

                # Now we've handled our dead minon.
                minion.handled = True

        # Unhandled minions remain (dead are garbage collected)
        self.minions = [minion for minion in self.minions if not minion.handled]

    def run(self):
        """ General work done by a minion moving stage.

        You should not need to touch this, unless you want the stage to behave differently.
        e.g. Not use minions or use minions differently."""

        # Run some methods to setup this stage (before moving minons around).
        self.start()

        # ################### Threaded Implementation ################################
        # Minions are cleaned up and created up to a batch_size maximum.

        # Handle minions
        self.handle_minions()

        # How many more minions can we have
        slots_available = self.batch_size - len(self.minions)

        # How many more items are there to-do, vs minions we're allowed
        attempt = min(len(self.todo), slots_available)

        # Start an attempt
        for i in range(attempt):

            # Lock
            suburb_task = self.todo.pop(0)
            # Unlock

            # Threaded
            # Create our minion to do our task
            minion = self.make_minion(suburb_task)

            # Add it to our list to keep track of them.
            self.minions.append(minion)

            # Tell it to run
            minion.start()

        # ############### Non Threaded Implementation ################################
        # Alternatively, we could do steps sequentially here (without minions)

        # success = False
        # try:
        #     success = self.work(task) # You will also need to define a work() method.
        # except:
        #     success = False
        # if success:
        #     self.done.append(item)
        # else:
        #     self.error.append(item)

        self.end()

    def start(self):
        """ Setup the stage run. """

        # print("Stage:", self.name)
        pass

    def end(self):
        """ Finalise the stage run. """

        # print("End Stage:", self.name)
        pass

    def handle_failed(self, task):
        """ Handles failed tasks.

        Override this method if you need to do more than dump failed items into the error list. (Default behaviour)
        This could be the place to implement a retry or some error correcting process if required.

        :param task: The task that failed.
        :return: Nothing.
        """

        self.error.append(task)

    def make_minion(self, item):
        """ Creates a minion for this stage. Override this to launch your specific stage-minion."""

        # Generic Stage, creates a generic minion. Override this.
        return Minion(args=(item,))




