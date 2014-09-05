
import os
import traceback
from nebula.dag import TaskDag, DagSet, TargetFile
from nebula.exceptions import CompileException
from nebula.scheduler import Scheduler
import nebula.tasks


class NebulaCompile:
    def __init__(self):
        self.target_map = {}

    def build_target(self, cls):
        def init(name, *args, **kwds):
            if name in self.target_map:
                raise CompileException("Duplicate Target Name: %s" % (name))
            try:
                inst = cls(name, *args, **kwds)
                self.target_map[name] = inst
            except Exception:
                traceback.print_exc()
                raise Exception("Failed to init: %s" % (cls) )
            return inst
        return init

    def compile(self, path):
        basedir = os.path.dirname(path)
        with open(path) as handle:
            code = handle.read()

        global_env = {
            'TargetFile' : TargetFile
        }
        
        for k, v in nebula.tasks.__mapping__.items():
            global_env[k] = self.build_target(v)

        local_env = {}
        my_code_AST = compile(code, "NebulaFile", "exec")
        try:
            old_cwd = os.getcwd()
            os.chdir(basedir)
            exec(my_code_AST, global_env, local_env)
        except CompileException, e:
            sys.stderr.write("Failure in Compile:" + e.msg + "\n")
            return 1
        finally:
            os.chdir(old_cwd)

    def to_dags(self):
        dag_map = {}
        
        all_targets = {}
        for k, v in self.target_map.items():
            all_targets[k] = v
            for sk, sv in v.sub_targets().items():
                all_targets[sk] = sv
        
        for i, k in enumerate(all_targets):
            dag_map[k] = i

        #build target->depends edges
        edges = []
        for key, value in all_targets.items():
            for r in value.requires():
                edges.append( (key, r.task_id) )

        change = True
        while change:
            change = False
            #find nodes that are connected but part of different DAG sets
            for dst, src in edges:
                if dag_map[src] != dag_map[dst]:
                    change = True
                    new_color = dag_map[src]
                    old_color = dag_map[dst]
                    cset = []
                    for node, color in dag_map.items():
                        if color == old_color:
                            cset.append(node)
                    for node in cset:
                        dag_map[node] = new_color

        out = DagSet()
        for i in set(dag_map.values()):
            d = TaskDag( i, dict([ (k,v) for k, v in self.target_map.items() if dag_map[k] == i ]) )
            out.append(d)

        return out
