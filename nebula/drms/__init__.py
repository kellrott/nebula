
import logging


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
