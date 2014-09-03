
import logging

class NotImplementedException(Exception):

    def __init__(self):
        Exception.__init__(self)

class DRMSWrapper(object):
    def __init__(self, scheduler, config):
        self.scheduler = scheduler
        self.config = config

    def worker_requests_active():
        return not self.schedular.done()

    def get_worker(self):
        raise NotImplementedException()

    def start(self):
        raise NotImplementedException()

    def stop(self):
        raise NotImplementedException()

    def work_loop(self):
        logging.info("Starting DRMS workloop")

        try:
            while not self.scheduler.done():
                job = None
                worker = self.get_worker()
                job = self.get_work(worker.id)
                if job is None:
                    time.sleep(5)
                else:
                    job.start()
        finally:
            self.stop()
