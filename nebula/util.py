
import hashlib
import uuid

__mapping__ = {
    'Galaxy' : ('nebula.galaxy', 'GalaxyService'),
}

def service_from_dict(data):
    i = __import__(__mapping__[data['service_type']][0])
    cls = getattr(i, __mapping__[data['service_type']][1])
    return cls.from_dict(data)



def file_uuid(path):
    """Generate a UUID from the SHA-1 of file."""
    hash = hashlib.sha1()
    with open(path, 'rb') as handle:
        while True:
            block = handle.read(1024)
            if not block: break
            hash.update(block)
    return uuid.UUID(bytes=hash.digest()[:16], version=5)


