
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.



from nebula.core import Service, ServiceConfig, Target, Task, TaskJob, TaskGroup, TargetFuture, TargetFile

__all__ = ['Service', 'ServiceConfig', 'Target', 'Task', 'TaskJob', 'TargetFuture', 'TargetFile', 'TaskGroup']

class NotImplementedException(Exception):
    def __init__(self):
        Exception.__init__(self)


class CompileException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.msg = msg

