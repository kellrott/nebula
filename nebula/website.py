
import os
import threading
import logging
import tornado.web



MAIN_PAGE="""
<html>
<script src="http://d3js.org/d3.v3.min.js" charset="utf-8"></script>

<body>

<h1><center>Grid Configuration</center><h1>

<script>

var mesos_offers = {
    'host1' : 10,
    'host2' : 20,
    'host3' : 30
}


d3.select("body")
    .append("svg")
    .attr("width", 50)
    .attr("height", 50)
    .append("circle")
    .attr("cx", 25)
    .attr("cy", 25)
    .attr("r", 25)
    .style("fill", "purple");
</script>

</body>
</html>
"""


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(MAIN_PAGE)

class ResourceHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler):
        self.scheduler = scheduler

    def get(self, path):
        if path == "nebula_executor.py":
            file = os.path.join(os.path.dirname(__file__), "nebula_executor.py")
            with open(file, 'rb') as f:
                while 1:
                    data = f.read(16384)
                    if not data: break
                    self.write(data)
            self.finish()
        else:
            self.write("error....")


class ServerThread(threading.Thread):
    def __init__(self, scheduler, port):
        self.scheduler = scheduler
        self.port = port
        self.instance = None
        threading.Thread.__init__(self)

    def run(self):
        application = tornado.web.Application([
            (r"/", MainHandler),
            (r"/resources/(.*)$", ResourceHandler, {'scheduler' : self.scheduler})
        ])

        application.listen(self.port)
        self.instance = tornado.ioloop.IOLoop.instance()
        self.instance.start()

    def stop(self):
        if self.instance is not None:
            self.instance.stop()


class WebSite:

    def __init__(self, scheduler, config):
        self.scheduler = scheduler
        self.config = config
        self.server = None

    def start(self):
        logging.info("starting website")
        self.server = ServerThread(self.scheduler, self.config.port)
        self.server.start()

    def stop(self):
        logging.info("stopping website")
        self.server.stop()
