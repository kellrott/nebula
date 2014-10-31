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
import logging
import tarfile
import logging

class Docker(object):
    def __init__(self, path):
        if os.path.exists(path):
            path = os.path.abspath(path)
            self.name = os.path.basename(path)
            self.path = path
        else:
            self.path = None
            self.name = path



def install(src_tar):
    src_name = re.sub(r'.tar$', '', os.path.basename(src_tar))
    t = tarfile.TarFile(src_tar)
    meta_str = t.extractfile('repositories').read()
    meta = json.loads(meta_str)

    cmd = "docker images --no-trunc"
    if not args.skip_sudo:
        cmd = "sudo " + cmd
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    stdo, stde = proc.communicate()
    tag, tag_value = meta.items()[0]
    rev, rev_value = tag_value.items()[0]
    found = False
    for line in stdo.split("\n"):
        tmp = re.split(r'\s+', line)
        if tmp[0] == tag and tmp[1] == rev and tmp[2] == rev_value:
            found = True
    if not found:
        logging.info("Installing %s" % (src_name))
        cmd = "docker load"
        if not args.skip_sudo:
            cmd = "sudo " + cmd
        cmd = "cat %s | %s" % (src_tar, cmd)
        subprocess.check_call(cmd, shell=True)
    else:
        logging.info("Already Installed: %s" % (src_name))

