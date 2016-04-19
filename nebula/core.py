
"""
Core Nebula Concepts
"""

import os
import uuid
import json
import time
import logging

from threading import Thread, RLock
from nebula.exceptions import NotImplementedException, CompileException
from nebula.util import task_from_dict, file_uuid
import traceback

class Target(object):
    def __init__(self, uuid):
        self.id = uuid
        self.uuid = uuid

    def to_dict(self):
        return {
            "model_class" : "Target",
            'uuid' : self.id
        }

    def __str__(self):
        return "uuid:%s" % (self.id)

    def __repr__(self):
        return '{"uuid":"%s"}' % (self.id)

    def __eq__(self, e):
        if isinstance(e, Target) and e.id == self.id:
            return True
        return False

class TargetFile(Target):
    def __init__(self, path):
        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            raise CompileException("File Not Found: %s" % (self.path))
        super(TargetFile, self).__init__(str(file_uuid(self.path)))
        self.parent_task_id = None


class TargetFuture(object):
    """
    Task output that will be generated in the future
    """
    def __init__(self, parent_task_id, in_uuid=None):
        if not isinstance(parent_task_id, basestring):
            print parent_task_id
            raise Exception("Non-String parent ID")
        self.parent_task_id = parent_task_id
        if in_uuid is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = in_uuid

    def to_dict(self):
        return {
            'task_id' : self.parent_task_id,
            'uuid' : str(self.id)
        }


class Task(object):
    def __init__(self):
        pass

    def get_inputs(self):
        raise NotImplementedException()

    def environment(self):
        raise NotImplementedException()

    def is_valid(self):
        raise NotImplementedException()

class TaskGroup(object):
    def __init__(self):
        self.tasks = []

    def append(self, task):
        self.tasks.append(task)

    def to_dict(self):
        return list(a.to_dict() for a in self.tasks)

    def store(self, handle):
        for a in self.tasks:
            handle.write(json.dumps(a.to_dict()) + "\n")

    def load(self, handle):
        for line in handle:
            self.tasks.append(task_from_dict(json.loads(line)))

    def __len__(self):
        return len(self.tasks)

    def __iter__(self):
        return self.tasks.__iter__()



class Engine(Thread):
    """

    """
    def __init__(self, name):
        super(Engine, self).__init__()
        self.name = name
        self.queue_lock = RLock()
        self.queue = {}
        self.active = {}

        self.running = True
        self.job_count = 0
        self.exception = None
        self.exception_str = None

    def get_config(self):
        raise NotImplementedException()

    def submit(self, task):
        with self.queue_lock:
            j = self.job_count
            job = TaskJob(j, task)
            self.job_count += 1
            self.queue[j] = job
            return job

    def get_queued(self):
        with self.queue_lock:
            if len(self.queue):
                job_id, job = self.queue.popitem()
                self.active[job_id] = job
                return job_id, job
        return None

    def is_ready(self):
        """
        """
        return True

    def run(self):
        try:
            self.runEngine()
        except Exception, exc:
            self.exception_str = traceback.format_exc()
            logging.error("Engine Failure:" + str(exc))
            print self.exception_str
            self.exception = exc

    def in_error(self):
        if self.exception is not None:
            print self.exception_str
        return self.exception is not None

    def stop(self):
        self.running = False

    def wait(self, items):
        sleep_time = 1
        collected = []
        while True:
            waiting = False
            #print "Waiting", items
            for i in items:
                status = self.status(i.job_id)
                #print "Status", i, status, self.is_ready()
                logging.info("Status check %s %s" % (status, i))
                if status in ['ok'] and i.job_id not in collected:
                    logging.info("Collecting outputs of %s" % (i.job_id))
                    meta_collect_count = 0
                    for name, dataset in i.get_outputs(all=True).items():
                        self.store_meta(dataset, self.docstore)
                        meta_collect_count += 1
                    #only store data for non-hiddent results
                    data_collect_count = 0
                    for name, dataset in i.get_outputs(all=True).items():
                        self.store_data(dataset, self.docstore)
                        data_collect_count += 1
                    collected.append(i.job_id)
                    logging.info("Collected: %d meta %d data",
                            meta_collect_count, data_collect_count
                    )
                if status in ['error'] and i.job_id not in collected:
                    logging.info("Collecting error output of %s" % (i.job_id))
                    for name, dataset in i.get_outputs(all=True).items():
                        self.store_meta(dataset, self.docstore)
                    collected.append(i.job_id)
                if status not in ['ok', 'error', 'unknown']:
                    waiting = True
            if not waiting:
                break
            if self.in_error():
                raise Exception("Service Error")
            time.sleep(sleep_time)
            if sleep_time < 60:
                sleep_time += 1
        logging.info("Waiting Done")

    def status(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return "waiting"
            if job_id in self.active:
                return self.active[job_id].state
            return "unknown"

    def get_job(self, job_id):
        with self.queue_lock:
            if job_id in self.queue:
                return self.queue[job_id]
            if job_id in self.active:
                return self.active[job_id]
        return None

    def store_data(self, data, object_store):
        raise NotImplementedException()

    def store_meta(self, data, object_store):
        raise NotImplementedException()


class TaskJob(object):
    def __init__(self, job_id, task):
        self.task = task
        #self.service_name = self.task['service']
        self.history = None
        self.outputs = {}
        self.hidden = {}
        self.error_msg = None
        self.job_id = job_id
        self.state = "queued"

    def set_running(self):
        self.state = "running"

    def set_done(self):
        self.state = "ok"

    def set_error(self, msg="Failure"):
        self.error_msg = msg
        self.state = 'error'

    def get_inputs(self):
        return self.task.get_inputs()

    def get_outputs(self, all=False):
        out = dict(self.outputs)
        if all:
            out.update(self.hidden)
        return out

    def get_status(self):
        return self.state