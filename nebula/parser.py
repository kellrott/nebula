
import sys
import os
import uuid
import traceback
import logging

from nebula.dag import TaskDag, TaskNode, DagSet, TargetFile, TargetFuture, TaskFuture
from nebula.exceptions import CompileException
from nebula.scheduler import Scheduler
from nebula.service import Docker
import nebula.tasks


class NebulaCompile:
    def __init__(self):
        self.target_map = {}
        self.output_map = {}

    def build_task(self, cls):
        def init(name, *args, **kwds):
            if name in self.target_map:
                raise CompileException("Duplicate Target Name: %s" % (name))
            try:
                inst = cls(name, *args, **kwds)
                self.target_map[name] = inst
            except Exception:
                traceback.print_exc()
                raise Exception("Failed to init: %s" % (cls) )
            return TaskFuture(inst)
        return init

    def yield_target(self, name, target):
        if not isinstance(target, TargetFuture):
            raise CompileException("Trying to yield non-target")
        self.output_map[name] = target

    def compile(self, path, additional_vars=None):
        self.src_path = path
        basedir = os.path.dirname(path)
        logging.info("Compiling: %s" % (path))
        with open(path) as handle:
            code = handle.read()

        global_env = {
            'TargetFile' : TargetFile,
            'Docker' : Docker,
            'Yield' : self.yield_target
        }

        if additional_vars is not None:
            for k,v in additional_vars.items():
                global_env[k] = v

        for k, v in nebula.tasks.__mapping__.items():
            global_env[k] = self.build_task(v)

        local_env = {}
        my_code_AST = compile(code, "NebulaFile", "exec")
        try:
            old_cwd = os.getcwd()
            os.chdir(basedir)
            exec(my_code_AST, global_env, local_env)
        except CompileException, e:
            os.chdir(old_cwd)
            sys.stderr.write("Failure in Compile:" + e.msg + "\n")
            return 1
        finally:
            os.chdir(old_cwd)

    def to_dags(self):
        #dag_map maps item node to its dag set
        dag_map = {}

        all_targets = {}
        for k, v in self.target_map.items():
            all_targets[k] = v

        for i, k in enumerate(all_targets):
            dag_map[k] = i

        #build src->depends edges
        edges = []
        for key, value in all_targets.items():
            for name, element in value.get_inputs().items():
                if isinstance(element, TargetFile) and element.parent_task_id is not None:
                    edges.append( (key, element.parent_task_id) )
                elif isinstance(element, TargetFuture) and element.parent_task_id is not None:
                    print "Parent_Task_id", element, element.parent_task_id
                    edges.append( (key, element.parent_task_id) )
                else:
                    raise Exception("Broken Input: %s" % element)
        change = True
        print "edges", edges
        print "elements", dag_map
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
            item_set = dict([ (k,v) for k, v in all_targets.items() if dag_map[k] == i ])
            if any( all_targets[t].is_active_task() for t in item_set ):
                d = TaskDag( i, item_set)
                out.append(d)

        return out
