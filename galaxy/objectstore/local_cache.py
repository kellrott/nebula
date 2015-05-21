
import os
import shutil
import logging
from galaxy.objectstore import ObjectStore, DiskObjectStore, directory_hash_id
from galaxy.util.files import umask_fix_perms

class CachedDiskObjectStore(ObjectStore):

    def __init__(self, config, cache_path, open_perms=False, config_xml=None, file_path=None, extra_dirs=None):
        self.disk = DiskObjectStore(config=config, config_xml=config_xml, file_path=file_path, extra_dirs=extra_dirs)
        self.cache_path = os.path.abspath(cache_path)
        self.open_perms = open_perms
        if not os.path.exists(self.cache_path):
            os.mkdir(self.cache_path)
        if self.open_perms:
            os.chmod(self.cache_path, 0o777)
        self._fix_permissions(self.cache_path)
        
    def _fix_permissions(self, rel_path):
        if self.open_perms:
            if os.path.isfile(rel_path):
                os.chmod(rel_path, 0o777)
            else:
                for basedir, _, files in os.walk(rel_path):
                    os.chmod(basedir, 0o777)
                    for filename in files:
                        path = os.path.join(basedir, filename)
                        # Ignore symlinks
                        if os.path.islink(path):
                            continue
                        os.chmod(path, 0o777)

    def create(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None, obj_dir=False):
        self.disk.create(obj,
            base_dir=base_dir, dir_only=dir_only,
            extra_dir=extra_dir, extra_dir_at_root=extra_dir_at_root,
            alt_name=alt_name, obj_dir=obj_dir)

    def update_from_file(self, obj, file_name=None, create=False, **kwargs):
        self.disk.update_from_file(obj=obj, file_name=file_name, create=create, **kwargs)

    def exists(self, obj, **kwargs):
        return self.disk.exists(obj=obj, **kwargs)

    def get_filename(self, obj, **kwargs):
        path_dir = self._cache_path_dir(obj)
        if not os.path.exists(path_dir):
            os.mkdir(path_dir)
            self._fix_permissions(path_dir)
        local_path = self._cache_path(obj)
        if not os.path.exists(local_path):
            logging.info("Caching %s" % (obj.id))
            shutil.copy( self.disk.get_filename(obj), local_path )
        self._fix_permissions(local_path)
        return local_path

    def _cache_path_dir(self, obj):
        return os.path.join(self.cache_path, *directory_hash_id(obj.id))

    def _cache_path(self, obj):
        return os.path.join(self._cache_path_dir(obj), "dataset_%s.dat" % obj.id)

    def _pull_into_cache(self, rel_path):
        # Ensure the cache directory structure exists (e.g., dataset_#_files/)
        rel_path_dir = os.path.dirname(rel_path)
        if not os.path.exists(self._get_cache_path(rel_path_dir)):
            os.makedirs(self._get_cache_path(rel_path_dir))
        # Now pull in the file
        file_ok = self._download(rel_path)
        self._fix_permissions(self._get_cache_path(rel_path_dir))
        return file_ok

    def local_cache_base(self):
        return self.cache_path
