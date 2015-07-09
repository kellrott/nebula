

import pymongo
import pymongo.errors


def setup_mongodb(host_ip):
    call_docker_run(
        "mongo", ports={27017:27017},
        name="nebula_test_mongo"
        )
    mongo_url = "mongodb://%s:27017" % (self.host_ip)
    time.sleep(10)
    for i in range(10):
        try:
            logging.info("Contacting: %s" % (mongo_url))
            client = pymongo.MongoClient(mongo_url)
            return mongo_url
        except pymongo.errors.ConnectionFailure:
            time.sleep(3)
    raise Exception("Unable to contact mongo db")
        