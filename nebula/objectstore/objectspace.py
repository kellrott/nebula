

import os
import string
import subprocess
from urllib import urlopen
from nebula.objectstore import ObjectStore, directory_hash_id


class ObjectSpaceFile(ObjectStore):

    def __init__(self, config, server_url, config_xml=None, file_path=None, **kwargs):
        self.file_path = file_path
        self.server_url = server_url
        self.running = True
        self.extra_dirs = {}

    def shutdown(self):
        self.running = False

    def exists(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        raise NotImplementedError()

    def file_ready(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        data_dir = os.path.join(base_dir, directory_hash_id(obj.uuid))
        return os.path.exists(data_dir)

    def create(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        raise NotImplementedError()

    def empty(self, obj, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        raise NotImplementedError()

    def size(self, obj, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        url = ANALYSIS_BASE + obj.uuid
        docstr = urlopen(url).read()
        dom = parseString(docstr)
        root_node = dom.childNodes[0]
        size = 0L
        for node, prefix, attrs, text in dom_scan(root_node, "ResultSet/Result/files/file/filesize"):
            size += long(text)
        return size

    def delete(self, obj, entire_dir=False, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        raise NotImplementedError()

    def get_data(self, obj, start=0, count=-1, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        raise NotImplementedError()

    def get_filename(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        data_dir = os.path.join(self.file_path, *directory_hash_id(obj.uuid))
        params = {
            'keyfile' : self.keyfile,
            'file_id' : obj.uuid,
            'data_dir' : data_dir,
            'uid' : str(os.getuid())
        }
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        if self.docker_config is not None:
            params['gtdownload'] = self.docker_config['image']
            if self.keyfile.startswith("http"):
                cmd = string.Template("docker run -i --rm -v ${data_dir}:/download -w /download -u ${uid} ${gtdownload} bash -c 'sleep 1 && gtdownload -c ${keyfile} -v -d ${file_id}'").substitute(params)
            else:
                cmd = string.Template("docker run -i --rm -v ${data_dir}:/download -w /download -v ${keyfile}:/keyfile.txt -u ${uid} ${gtdownload} bash -c 'sleep 1 && gtdownload -c /keyfile.txt -v -d ${file_id}'").substitute(params)
        else:
            cmd = string.Template("gtdownload -c ${keyfile} -p ${data_dir} -v -d ${file_id} -u ${uid}").substitute(params)
        subprocess.check_call(cmd, shell=True)
        bam_file = None
        for a in glob(os.path.join(params['tumor_id'], "*.bam")):
            bam_file = a
        return bam_file

    def update_from_file(self, obj, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None, file_name=None, create=False):
        raise NotImplementedError()

    def get_object_url(self, obj, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        return None

    def get_store_usage_percent(self):
        return 1.0
