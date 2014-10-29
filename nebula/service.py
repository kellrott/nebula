

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

import os


class Service(object):
    def __init__(self, name):
        self.name = name

class Docker(Service):    
    def __init__(self, path, name=None):
        path = os.path.abspath(path)
        if name is None:
            name = os.path.basename(path)
        self.path = path
        super(Docker, self).__init__(name)


class Galaxy(Service):
    def __init__(self, path, name=None):
        path = os.path.abspath(path)
        if name is None:
            name = os.path.basename(path)
        self.path = path
        super(Galaxy, self).__init__(name)

