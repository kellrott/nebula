#!/usr/bin/env python

import mesos
import mesos_pb2

from argparse import ArgumentParser
import sys
import time
import os
import socket
import json
import threading
import traceback
import logging

logging.basicConfig(level=logging.INFO)


class SubTask(object):
    def __init__(self, driver, task):
        self.driver = driver
        self.task = task

    def run(self):
        logging.info("Running align task: %s" % (self.task.task_id.value))
        try:
            obj = json.loads(self.task.data)

            #launch and wait on galaxy here

            update = mesos_pb2.TaskStatus()
            update.task_id.value = self.task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            self.driver.sendStatusUpdate(update)
        except Exception, e:
            traceback.print_exc()
            update = mesos_pb2.TaskStatus()
            update.task_id.value = self.task.task_id.value
            update.state = mesos_pb2.TASK_FAILED
            self.driver.sendStatusUpdate(update)


class GalaxyFarmExecutor(mesos.Executor):
    def __init__(self):
        mesos.Executor.__init__(self)

    def init(self, driver, arg):
        logging.info("Starting task worker")

    def launchTask(self, driver, task):
        logging.debug( "Running task %s" % task.task_id.value )
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task.task_id.value
        update.state = mesos_pb2.TASK_RUNNING

        subtask = SubTask(driver, task)
        threading.Thread(target=subtask.run).start()
        driver.sendStatusUpdate(update)

    def killTask(self, driver, task_id):
        logging.debug( "Killing task %s" % task_id.value )
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        update.data = json.dumps( { 'hostname' : socket.gethostname(), 'task_id' : task_id.value } )
        driver.sendStatusUpdate(update)

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        if message == "diskspace":
            pass

    def shutdown(self, driver):
        logging.debug( "shutdown" )
        #cleanup()

    def error(self, driver, code, message):
        print "Error: %s" % message

if __name__ == "__main__":
    logging.info( "Starting Workflow Watcher" )
    executor = GalaxyFarmExecutor()
    mesos.MesosExecutorDriver(executor).run()
