

import os
import json
import logging

from nebula.tasks import Task
from nebula.target import Target, TargetFuture


class MD5TargetFuture(TargetFuture):

    def __init__(self, task_id, step_id, output_name):
        self.step_id = step_id
        self.output_name = output_name
        super(MD5TargetFuture, self).__init__(task_id)

class MD5Task(Task):
    def __init__(self, input=None):

        self.input = input
