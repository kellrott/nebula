
import os
import string
import subprocess
from urllib import urlopen
from xml.dom.minidom import parseString
from nebula.objectstore import ObjectStore, directory_hash_id


def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


def dom_scan(node, query):
    stack = query.split("/")
    if node.localName == stack[0]:
        return dom_scan_iter(node, stack[1:], [stack[0]])

def dom_scan_iter(node, stack, prefix):
    if len(stack):
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if child.localName == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [stack[0]]):
                        yield out
                elif '*' == stack[0]:
                    for out in dom_scan_iter(child, stack[1:], prefix + [child.localName]):
                        yield out
    else:
        if node.nodeType == node.ELEMENT_NODE:
            yield node, prefix, dict(node.attributes.items()), getText( node.childNodes )
        elif node.nodeType == node.TEXT_NODE:
            yield node, prefix, None, getText( node.childNodes )



ANALYSIS_BASE="https://cghub.ucsc.edu/cghub/metadata/analysisFull/"

class GNOSStore(ObjectStore):

    def __init__(self, config, config_xml=None, file_path=None, keyfile=None, docker_config=None,**kwargs):
        self.file_path = file_path
        self.running = True
        if keyfile is not None:
            self.keyfile=keyfile
        else:
            self.keyfile="https://cghub.ucsc.edu/software/downloads/cghub_public.key"
        self.docker_config = docker_config
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
