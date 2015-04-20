
import uuid
import json


class Target(object):
    def __init__(self, uuid):
        self.uuid = uuid

    def to_dict(self):
        return {
            "model_class" : "Target",
            'uuid' : self.uuid
        }

    def __str__(self):
        return "uuid:%s" % (self.uuid)

    def __repr__(self):
        return '{"uuid":"%s"}' % (self.uuid)

    def __eq__(self, e):
        if isinstance(e, Target) and e.uuid == self.uuid:
            return True
        return False

class TargetFile(Target):
    def __init__(self, path):
        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            raise CompileException("File Not Found: %s" % (self.path))
        super(TargetFile,self).__init__(str(file_uuid(self.path)))
        self.parent_task_id = None


class TargetFuture(object):
    """
    Task output that will be generated in the future
    """
    def __init__(self, parent_task_id, in_uuid=None):
        if not isinstance(parent_task_id, basestring):
            print parent_task_id
            raise CompileException("Non-String parent ID")
        self.parent_task_id = parent_task_id
        if in_uuid is None:
            self.uuid = str(uuid.uuid4())
        else:
            self.uuid = in_uuid

    def to_dict(self):
        return {
            'task_id' : self.parent_task_id,
            'uuid' : str(self.uuid)
        }
