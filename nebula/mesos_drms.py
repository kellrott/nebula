
import logging
import threading

import mesos
import mesos_pb2

import nebula.drms

class MesosDRMS(nebula.drms.DRMSWrapper):

    def __init__(self, scheduler, config):
        super(MesosDRMS, self).__init__(scheduler, config)

        if self.config.mesos is None:
            logging.error("Mesos not configured")
            return
        self.sched = NebularMesos(scheduler, config)
        self.framework = mesos_pb2.FrameworkInfo()
        self.framework.user = "" # Have Mesos fill in the current user.
        self.framework.name = "Nebula"
        ## additional authentication stuff would go here
        self.driver = mesos.MesosSchedulerDriver(self.sched, self.framework, self.config.mesos)

    def start(self):
        logging.info("Starting Mesos Thread")
        self.driver_thread = DriverThread(self.driver)
        self.driver_thread.start()

    def stop(self):
        logging.info("Stoping Mesos Thread")
        self.driver_thread.stop()

class DriverThread(threading.Thread):
    def __init__(self, driver):
        threading.Thread.__init__(self)
        self.driver = driver

    def run(self):
        self.driver.run() #this doesn't return until stop is called by another thread

    def stop(self):
        self.driver.stop()

class NebularMesos(mesos.Scheduler):
    """
    The GridScheduler is responsible for deploying and managing child Galaxy instances using Mesos
    """
    def __init__(self, scheduler, config):
        mesos.Scheduler.__init__(self)
        self.scheduler = scheduler
        self.config = config
        self.hosts = {}
        server = "localhost"
        self.master_url = "http://%s:%d" % (server, config.port)
        logging.info("Starting Mesos scheduler")
        logging.info("Mesos Resource URL %s" % (self.master_url))


    def getExecutorInfo(self):
        """
        Build an executor request structure
        """

        uri_value = "http://localhost:8080/static/galaxy_farm_worker.py" #bug, need to correctly form this URI

        logging.info("in getExecutorInfo, setting execPath = " + uri_value)
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "galaxy_farm_worker"

        uri = executor.command.uris.add()
        uri.value = uri_value
        uri.executable = True

        executor.command.value = "./galaxy_farm_worker.py"
        executor.name = "galaxy_farm_worker"
        executor.source = "galaxy_farm"
        return executor

    def getTaskInfo(self, offer, task_name, worker_image, accept_cpu, accept_mem):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = task_name
        task.slave_id.value = offer.slave_id.value
        task.name = "Galaxy Worker"
        task.executor.MergeFrom(self.getExecutorInfo())

        task_data = {
            'galaxy_tarball' : 'http://localhost:8080/static/workers/%s.tar.gz' % (worker_image),
            'galaxy_name' : worker_image,
            'galaxy_master' : self.master_url,
            'galaxy_farm_key' : ""
        }
        task.data = json.dumps(task_data)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = accept_cpu

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = accept_mem
        return task


    def registered(self, driver, fid, masterInfo):
        logging.info("Galaxy Grid registered with frameworkID %s" % fid.value)

    def resourceOffers(self, driver, offers):
        logging.debug("Got %s slot offers" % len(offers))
        batch_ready = {}
        #wq = WorkQueue(self.app.model.context)

        for offer in offers:
            #store the offer info
            cpu_count = 0
            for res in offer.resources:
                if res.name == 'cpus':
                    cpu_count = int(res.scalar.value)

            mem_count = 0
            for res in offer.resources:
                if res.name == 'mem':
                    mem_count = int(res.scalar.value)

            tasks = []

            work = self.scheduler.get_work(offer.hostname)
            if work is not None:
                logging.info("Starting work: %s" % (work))
            """
            self.comm.setComputeResourceInfo( ComputeResource(offer.hostname, cpu_count, mem_count) )
            if offer.hostname not in self.hosts:
                farm_request = wq.get_farm_request()
                if farm_request is not None:
                    logging.debug("Offered %d cpus" % (cpu_count))
                    cpu_request = 0
                    cpu_slice = 1
                    mem_slice = 1024
                    task_name = "galaxy_worker:%s:%s" % (farm_request.batch.id, offer.hostname)
                    task = self.getTaskInfo(offer, task_name, farm_request.batch.worker_image, cpu_slice, mem_slice)
                    cpu_request += cpu_slice
                    tasks.append(task)
                    self.hosts[offer.hostname] = self.hosts.get(offer.hostname, 0) + cpu_slice
            """
            status = driver.launchTasks(offer.id, tasks)


    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_RUNNING:
            logging.info("Task %s, slave %s is RUNNING" % (status.task_id.value, status.slave_id.value))

        if status.state == mesos_pb2.TASK_FINISHED:
            logging.info("Task %s, slave %s is FINISHED" % (status.task_id.value, status.slave_id.value))


    def getFrameworkName(self, driver):
        return "GalaxyGrid"
