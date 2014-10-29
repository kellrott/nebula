
import os
import logging
import hashlib
import uuid
import itertools

def file_uuid(path):
    """Generate a UUID from the SHA-1 of file."""
    hash = hashlib.sha1()
    with open(path, 'rb') as handle:
        while True:
            block = handle.read(1024)
            if not block: break
            hash.update(block)
    return uuid.UUID(bytes=hash.digest()[:16], version=5)


class DataManager:
    def __init__(self, output_dir, shared_dirs):
        self.output_dir = output_dir
        self.shared_dirs = list( os.path.abspath(a) for a in shared_dirs )
        self.data_locations = {}

    def add_file(self, data_id, path):
        path = os.path.abspath(path)
        shared = False
        for a in self.shared_dirs:
            if path.startswith(a):
                shared = True

        if shared:
            self.add_location(data_id, "*")
        else:
            self.add_location(data_id, "localhost")

    def has_data(self, data_id):
        return data_id in self.data_locations

    def get_locations(self, data_id):
        return self.data_locations.get(data_id, [])

    def add_location(self, data_id, host):
        logging.debug("Adding data %s location %s" % (data_id, host))
        self.data_locations[data_id] = set(itertools.chain(self.data_locations.get(data_id, set()), [host]))
