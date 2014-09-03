
from dag import WorkflowFuture
from scheduler import Scheduler

class NebulaCompile:
    def __init__(self):
        self.target_map = {}

    def build_target(self, cls):
        def init(name, *args):
            if name in self.target_map:
                raise CompileException("Duplicate Target Name: %s" % (name))
            inst = cls(name, *args)
            self.target_map[name] = inst
            return inst
        return init

    def compile(self, path):
        with open(path) as handle:
            code = handle.read()

        global_env = {
            'workflow' : self.build_target(WorkflowFuture)
        }

        local_env = {}
        my_code_AST = compile(code, "NebulaFile", "exec")
        try:
            exec(my_code_AST, global_env, local_env)
        except CompileException, e:
            sys.stderr.write("Failure in Compile:" + e.msg + "\n")
            return 1
