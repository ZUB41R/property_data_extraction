#  Copyright (C) 2021
#  Author: Zubair Ahmed <zub41r.ahm3d@gmail.com>

from utils.helper_functions import loadpickle, dumppickle, create_dir
from scraper.stages.suburb_load import SuburbLoadStage
from scraper.stages.store import StoreStage
from scraper.stages.stage import Stage, Minion
from utils.suburb_task import SuburbTask

import os
import time


class Pipeline(object):
    """A pipeline runs Tasks through a series of (fixed) stages."""

    def __init__(self, logger, config, database_conn):
        """Every time a pipeline is initiated, there are a few items to setup."""

        # Keep a copy of the configuration
        self.logger = logger
        self.config = config
        self.database_conn = database_conn
        self.stage_list = self.config.stage_list
       
        self.stages = []
        self.ordered_stages = []

        # A Done stage holds all the completed work from this pipeline
        self.done_stage = Stage("Done")

        # An Error stage holds all the tasks that have error-ed out in any stage.
        self.error_stage = Stage("Errors")

        # save the scans as jpgs in a cache
        if "suburb_load" in self.stage_list :
            suburb_details_stage = SuburbLoadStage(self.config)
            self.stages.append(suburb_details_stage)
            self.ordered_stages.append("suburb_details")

        # store the results and scan info in the mldb (new postgres database)
        if "store" in self.stage_list :
            store_stage = StoreStage(self.config, self.database_conn)
            self.stages.append(store_stage)

        # ???   
        # publish_stage = PublishStage(self.config, self.database)
    
        # bench_stage = BenchStage(self.config, self.database)
    
        # grade_stage = GradeStage(self.config)

        # Define our default pipeline from hardcoded values.
        # TODO: This could be configured from user input string (stored in Config.bin)

        # self.stages = [cache_stage, preprocess_stage, publish_stage]

        # Cache and segment
        # self.stages = [cache_stage, segment_stage, publish_stage]

        # Update Meta Pipeline
        # self.stages = [cache_stage, preprocess_stage, publish_stage]

        # Benchmarking
        # self.stages = [cache_stage, preprocess_stage, predict_stage, bench_stage]

        # cache and preprocess
        # self.stages = [cache_stage, preprocess_stage, publish_stage]

        # Update Grade Testing
        # self.stages = [cache_stage, preprocess_stage, grade_stage]
        self.logger.debug(f"Pipeline started with stages: {self.stages}")
    
    def run(self):
        """Running the pipeline means setting up, doing work and close out.

        Setup activities occur in the self.start() function.
        Running activities are defined here.
        Closeout activites occur in the self.end() function.
        """

        # If we don't have a false start
        if self.start():

            # Count the stage
            num_stages = len(self.stages)
            last_stage = num_stages-1

            # Run the work
            there_is_more_to_do = True
            prev_status_string = ""
            last_active = time.time()

            while there_is_more_to_do:

                # Assume there's nothing more to do, until defined otherwise.
                there_is_more_to_do = False

                # Status string
                status = []

                # For each stage
                for i in range(num_stages):

                    # Do some stuff: Check status, Run stage, Transfer Tasks around, Report status.
                    this_stage = self.stages[i]

                    # The counts show if the stage has tasks for the pipeline to move around.
                    todo_count = len(this_stage.todo)
                    done_count = len(this_stage.done)
                    error_count = len(this_stage.error)

                    # The stage is active if it meets some conditions
                    if this_stage.is_active():

                        # The pipeline then has more to do.
                        there_is_more_to_do = True

                    # Run this stage
                    this_stage.run()

                    # If the stage has [done] some tasks...
                    if done_count > 0:

                        # and its not the last stage
                        if i < last_stage:

                            # transfer these tasks to the next stage.
                            next_stage = self.stages[i+1]
                            self.transfer_tasks(this_stage, next_stage)

                        else:

                            # otherwise, transfer them to the done stage
                            self.transfer_tasks(this_stage, self.done_stage)

                    # If the stage has some [errors]...
                    if error_count > 0:

                        # transfer them to the (internal) error stage.
                        self.transfer_errors(this_stage, self.error_stage)

                    # If the stage is active
                    if this_stage.is_active():

                        # Report it as {Stage Name: to-do/running/error/done}
                        status.append("[{0:<3}:[{1:<3}/{2:<3}/{3:<3}/{4:<3}]] ".format(this_stage.name,
                                                                             len(this_stage.todo),
                                                                             len(this_stage.minions),
                                                                             len(this_stage.error),
                                                                             len(this_stage.done)))


                    else:
                        status.append("[{0:<3}:[{1:15}]] ".format(this_stage.name, "INACTIVE"))

                # Also report our error and done stages.
                status.append("[{0}:[{1:<5}]] ".format(self.error_stage.name, len(self.error_stage.todo)))
                status.append("[{0}:[{1:<5}]] ".format(self.done_stage.name, len(self.done_stage.todo)))

                # Compile the status of all our stages together.
                status_string = "".join(status)

                # Print the success string if there was any change in the pipeline
                # TODO: Make the pipeline faster by not hinging everything on string comparison (of a long string)
                print_status = status_string != prev_status_string
                time_now = time.time()

                # If we print the status, this was the time we were last active.
                if print_status:
                    last_active = time_now
                    print("{0:<20}:".format(last_active), status_string)
                elif (time_now - last_active) > 10:
                    # If its more than 10 seconds, we might be hung.
                    last_active = time_now
                    status_string = "Pipeline: Possible Deadlock (Inactive for 10s.)"
                    print("{0:<20}:".format(last_active), status_string)

                    # Print an error in deadlock
                    self.print_errors(1)

                # Make sure we keep track of the status for the next time around.
                prev_status_string = status_string

        # Do any closeout work
        self.end()

    def transfer_tasks(self, stage, next_stage):
        """ Transfer tasks from stage to the next_stage.

        :param stage: Tasks from this stage's done list will be moved.
        :param next_stage: Tasks will be placed into next_stage's to-do list.
        :return: Nothing is returned.
        """
        # TODO: Implement Locking? Ideal but may not be needed with this architecture.
        # Multi-threaded isn't really  in python because of the GIL.

        # Lock Stage.Done

        # Get items from stage.done
        prev_stage_done = stage.done

        # Clear (set a new list)
        stage.done = []

        # Unlock Stage.Done

        # Lock NextStage.to_do

        # Extend the list of work to do in the next stage
        next_stage.todo.extend(prev_stage_done)

        # Unlock Nextstage.to_do


    def transfer_errors(self, stage, next_stage):
        """ Transfer errors from a stage to the next_stage.

        Next_stage should typically be the pipeline's error_stage.

        :param stage: Tasks from this stage's error list will be moved.
        :param next_stage: Tasks will be placed into next_stage's to-do list.
        :return: Nothing is returned.
        """
        # TODO: Implement Locking?
        # TODO: Implement error reporting.

        # Lock Stage.Error

        # Get items from stage.done
        prev_stage_error = stage.error
        # Clear (set a new list)
        stage.error = []

        # Unlock Stage.Error

        # Lock NextStage.Todo

        # Extend the list of work to do in the next stage
        next_stage.todo.extend(prev_stage_error)

        # Unlock Nextstage.Error

    def start(self):
        """ Start the pipeline with some initial steps.

        Check things like this pipeline has tasks etc.

        :return: True if the pipeline started successfully. False otherwise.
        """
        # TODO: Remove print statements and use logger instead.
        # TODO: Start and limit should be observed by the worker and not the pipeline (multi-processing)

        # Display Spacer
        print("+++++++++++++ PIPELINE RUN +++++++++++++++++++++")

        # Get the first stage.
        start_stage = self.stages[0]

        # Its targets are the pipeline targets
        # suburb_targets = ["suburb"]
        # Targets may be strings (UUIDs) or scans (Json)
        suburb_targets_task = [SuburbTask(key) for key in self.config.suburb_base_urls]
        # print(f"suburb_targets_task: {suburb_targets_task[0].__dict__}")


        suburb_task_list = []
        for suburb_task in suburb_targets_task:
            suburb_task.urls = self.config.suburb_base_urls[suburb_task.name]
            # print(f"suburb_task.urls: {suburb_task.urls}")
            suburb_task_list.append(suburb_task)
        
        # if self.config.start is not None:
        #     targets = targets[self.config.start:]

        # if self.config.limit is not None:
        #     targets = targets[:self.config.limit]

        if suburb_task_list is None or len(suburb_task_list) == 0:

            print("No suburb_targets to run.")
            return False

        self.logger.debug(f"Loaded: {len(suburb_task_list)}, suburb_targets into  {start_stage.name}")

        # TODO: maybe self.to_do ? Depends on multi-processing implementation.
        # Load the targets into the first stage
        start_stage.todo = suburb_task_list

        return True

    def end(self):
        """ End the pipeline with some finalisation steps.

        :return: Nothing returned.
        """
        # TODO: Output a pipeline report?

        # Spacer
        print("+++++++++++++ PIPELINE END +++++++++++++++++++++")

        # Dump all errors
        self.dump_errors()

        # Show us 10 of the errors that occured:
        self.print_errors(10)

        return

    def __str__(self):
        """ Returns a string representation of this pipeline.

        At present, just shows the stages of the pipeline.

        :return: String representation of the pipeline configuration.
        """
        suburb_targets_string = "No Targets!"

        if self.config.suburb_main_dict["suburb"] is not None:
            suburb_targets_string = str(self.config.suburb_main_dict["suburb"])

        stage_string = ", ".join([stage.name for stage in self.stages])
        return "{suburb_targets_string} => [{stage_string}]".format(stage_string=stage_string, suburb_targets_string=suburb_targets_string)

    def show(self):
        """ Prints a string representation to the console.

        :return: Nothing
        """
        # TODO: Remove print statements and use logger.

        # Spacer
        print("==============PIPELINE==========================")
        # print(str(self))

    def convert(self, targets):
        """ Convert targets from fake string targets to actual Tasks.

        This method was useful during initial development to convert test strings into Tasks if they were peristed
        in the Targets.bin. This method can be deprecated anytime.

        :param targets: A list of targets, of type Task or string.
        :return: A list of Tasks with the targets inside.
        """

        # Converts Task strings into Tasks
        if len(targets) > 0:
            if isinstance(targets[0], SuburbTask):

                # The first element is a task. Already converted, do nothing.
                return targets

            elif isinstance(targets[0], str):

                # The first element is a string. Go through each and convert to Task.
                converted_targets = []

                for target in targets:
                    new_target = SuburbTask(target)

                    # # If this is a string, (pseduo-example) generate some test json data.
                    # new_target.json = self.database.get_json_test(target)

                    converted_targets.append(new_target)

                return converted_targets

            else:

                raise TypeError('Targets list is neither strings nor tasks. Could not convert to tasks.')

        else:
            return None

    def print_errors(self, count):
        """ Print the errors observed in this pipeline to a maximum of a specific count.

        This method is used to display a selection of errors after a run for diagnostic purposes.

        :param count: A integer representing the number of errors to print.
        """

        # TODO: Flesh out Scaf for better error management: output to file?
        # TODO: Output the errors as a new list of targets?

        for task in self.error_stage.todo:

            print("Error in:", repr(task.name), "with", repr(task.error))

            # Limiting the errors printed to console.
            count -= 1
            if count <= 0:
                return

        return

    def dump_errors(self):
        """Dump errored targets (.bin) and an error report (.csv) to disk."""

        # Get the time now.
        time_string_now = str(time.time())
        worker_string = self.config.name

        # TODO: Setup config option for logs.
        logs_directory = "logs"

        # Make the directory if it doesnt exist.
        create_dir(logs_directory)

        log_filename = time_string_now + "-" + worker_string + "-error-log"
        bin_filename = time_string_now + "-" + worker_string + "-error-targets"

        log_path = os.path.join(logs_directory, log_filename)
        bin_path = os.path.join(logs_directory, bin_filename)

        # For every target error-ed compile a csv
        with open(log_path + ".csv", 'a') as f:

            for task in self.error_stage.todo:
                f.write(repr(task.name) + "," + repr(task.error) + "\n")

        # Also dump (as a bin) the targets and errors.
        dumppickle(bin_path, self.error_stage.todo)
