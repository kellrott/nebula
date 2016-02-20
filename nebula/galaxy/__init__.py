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


"""
Methods to enable using Galaxy using Nebula
"""

import os
import json
from glob import glob
from xml.dom.minidom import parse as parseXML
from nebula.galaxy.galaxy_docker import GalaxyEngine
from nebula.galaxy.core import  GalaxyWorkflowTask, GalaxyResources
from nebula.galaxy.io import GalaxyWorkflow

__all__ = ['GalaxyEngine', 'GalaxyResources', 'GalaxyWorkflow', 'GalaxyWorkflowTask']
