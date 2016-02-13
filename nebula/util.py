
"""
Utility functions used in Nebula
"""

import hashlib
import uuid
import importlib

__task_mapping__ = {
    'GalaxyWorkflow' : ('nebula.galaxy.core', 'GalaxyWorkflowTask'),
}

__engine_mapping__ = {
    'GalaxyEngine' : ('nebula.galaxy.galaxy_docker', 'GalaxyEngine'),
}

def task_from_dict(data):
    """
    Create task class from dict based description
    """
    i = importlib.import_module(__task_mapping__[data['task_type']][0])
    print __task_mapping__[data['task_type']][0], i.__file__, dir(i)
    cls = getattr(i, __task_mapping__[data['task_type']][1])
    return cls.from_dict(data)

def engine_from_dict(data):
    """
    Create engine class from dict based description
    """
    i = importlib.import_module(__engine_mapping__[data['engine_type']][0])
    print __engine_mapping__[data['engine_type']][0], i.__file__, dir(i)
    cls = getattr(i, __engine_mapping__[data['engine_type']][1])
    return cls.from_dict(data)



def file_uuid(path):
    """Generate a UUID from the SHA-1 of file."""
    hasher = hashlib.sha1()
    with open(path, 'rb') as handle:
        while True:
            block = handle.read(1024)
            if not block:
                break
            hasher.update(block)
    return uuid.UUID(bytes=hasher.digest()[:16], version=5)



