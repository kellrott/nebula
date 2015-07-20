
import logging

"""
class DRMSWrapper(object):
    def __init__(self, scheduler, config):
        self.scheduler = scheduler
        self.config = config

    def start(self):
        raise NotImplementedException()

    def worker_requests_active():
        return not self.schedular.done()

    def get_worker(self):
        raise NotImplementedException()

    def stop(self):
        raise NotImplementedException()
"""

class MesosJob:
    def __init__(self, service, task, job_id):
        self.service = service
        self.task = task
        self.job_id = job_id
        self.name = service.name + ":" + task.task_id
    
    def to_dict(self):
        return {
            'service' : self.service.to_dict(),
            'task' : self.task.to_dict()
        }
        