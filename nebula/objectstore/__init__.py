
import os
import stat
import shutil


class ObjectStore(object):
    """
    ObjectStore abstract interface
    """
    def __init__(self, config, config_xml=None, **kwargs):
        self.running = True
        self.extra_dirs = {}

    def shutdown(self):
        self.running = False

    def exists(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Returns True if the object identified by `obj` exists in this file
        store, False otherwise.

        FIELD DESCRIPTIONS (these apply to all the methods in this class):

        :type obj: object
        :param obj: A Galaxy object with an assigned database ID accessible via
                    the .id attribute.

        :type base_dir: string
        :param base_dir: A key in self.extra_dirs corresponding to the base
                         directory in which this object should be created, or
                         None to specify the default directory.

        :type dir_only: bool
        :param dir_only: If True, check only the path where the file
                         identified by `obj` should be located, not the dataset
                         itself. This option applies to `extra_dir` argument as
                         well.

        :type extra_dir: string
        :param extra_dir: Append `extra_dir` to the directory structure where
                          the dataset identified by `obj` should be located.
                          (e.g., 000/extra_dir/obj.id)

        :type extra_dir_at_root: bool
        :param extra_dir_at_root: Applicable only if `extra_dir` is set.
                                  If True, the `extra_dir` argument is placed at
                                  root of the created directory structure rather
                                  than at the end (e.g., extra_dir/000/obj.id
                                  vs. 000/extra_dir/obj.id)

        :type alt_name: string
        :param alt_name: Use this name as the alternative name for the created
                         dataset rather than the default.
        """
        raise NotImplementedError()

    def file_ready(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """ A helper method that checks if a file corresponding to a dataset
        is ready and available to be used. Return True if so, False otherwise."""
        return True

    def create(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Mark the object identified by `obj` as existing in the store, but with
        no content. This method will create a proper directory structure for
        the file if the directory does not already exist.
        See `exists` method for the description of other fields.
        """
        raise NotImplementedError()

    def empty(self, obj, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Test if the object identified by `obj` has content.
        If the object does not exist raises `ObjectNotFound`.
        See `exists` method for the description of the fields.
        """
        raise NotImplementedError()

    def size(self, obj, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Return size of the object identified by `obj`.
        If the object does not exist, return 0.
        See `exists` method for the description of the fields.
        """
        raise NotImplementedError()

    def delete(self, obj, entire_dir=False, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Deletes the object identified by `obj`.
        See `exists` method for the description of other fields.

        :type entire_dir: bool
        :param entire_dir: If True, delete the entire directory pointed to by
                           extra_dir. For safety reasons, this option applies
                           only for and in conjunction with the extra_dir option.
        """
        raise NotImplementedError()

    def get_data(self, obj, start=0, count=-1, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Fetch `count` bytes of data starting at offset `start` from the
        object identified uniquely by `obj`.
        If the object does not exist raises `ObjectNotFound`.
        See `exists` method for the description of other fields.

        :type start: int
        :param start: Set the position to start reading the dataset file

        :type count: int
        :param count: Read at most `count` bytes from the dataset
        """
        raise NotImplementedError()

    def get_filename(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        Get the expected filename (including the absolute path) which can be used
        to access the contents of the object uniquely identified by `obj`.
        See `exists` method for the description of the fields.
        """
        raise NotImplementedError()

    def update_from_file(self, obj, base_dir=None, extra_dir=None, extra_dir_at_root=False, alt_name=None, file_name=None, create=False):
        """
        Inform the store that the file associated with the object has been
        updated. If `file_name` is provided, update from that file instead
        of the default.
        If the object does not exist raises `ObjectNotFound`.
        See `exists` method for the description of other fields.

        :type file_name: string
        :param file_name: Use file pointed to by `file_name` as the source for
                          updating the dataset identified by `obj`

        :type create: bool
        :param create: If True and the default dataset does not exist, create it first.
        """
        raise NotImplementedError()

    def get_object_url(self, obj, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """
        If the store supports direct URL access, return a URL. Otherwise return
        None.
        Note: need to be careful to to bypass dataset security with this.
        See `exists` method for the description of the fields.
        """
        raise NotImplementedError()

    def get_store_usage_percent(self):
        """
        Return the percentage indicating how full the store is
        """
        raise NotImplementedError()

    ## def get_staging_command( id ):
    ##     """
    ##     Return a shell command that can be prepended to the job script to stage the
    ##     dataset -- runs on worker nodes.
    ##
    ##     Note: not sure about the interface here. Should this return a filename, command
    ##     tuple? Is this even a good idea, seems very useful for S3, other object stores?
    ##     """




def directory_hash_id( id ):
    """

    >>> directory_hash_id( 100 )
    ['000']
    >>> directory_hash_id( "90000" )
    ['090']
    >>> directory_hash_id("777777777")
    ['000', '777', '777']
    """
    s = str( id ).replace("-", "")
    l = len( s )
    # Shortcut -- ids 0-999 go under ../000/
    if l < 4:
        return [ "000" ]
    if l < 10:
        # Pad with zeros until a multiple of three
        padded = ( ( 3 - len( s ) % 3 ) * "0" ) + s
        # Drop the last three digits -- 1000 files per directory
        padded = padded[:-3]
        # Break into chunks of three
        return [ padded[ i * 3 : (i + 1 ) * 3 ] for i in range( len( padded ) // 3 ) ]
    else:
        #assume it is a UUID
        return [ s[0:2], s[2:4] ]

def umask_fix_perms( path, umask, unmasked_perms, gid=None ):
    """
    umask-friendly permissions fixing
    """
    perms = unmasked_perms & ~umask
    try:
        st = os.stat( path )
    except OSError, e:
        log.exception( 'Unable to set permissions or group on %s' % path )
        return
    # fix modes
    if stat.S_IMODE( st.st_mode ) != perms:
        try:
            os.chmod( path, perms )
        except Exception, e:
            log.warning( 'Unable to honor umask (%s) for %s, tried to set: %s but mode remains %s, error was: %s' % ( oct( umask ),
                                                                                                                      path,
                                                                                                                      oct( perms ),
                                                                                                                      oct( stat.S_IMODE( st.st_mode ) ),
                                                                                                                      e ) )
    # fix group
    if gid is not None and st.st_gid != gid:
        try:
            os.chown( path, -1, gid )
        except Exception, e:
            try:
                desired_group = grp.getgrgid( gid )
                current_group = grp.getgrgid( st.st_gid )
            except:
                desired_group = gid
                current_group = st.st_gid
            log.warning( 'Unable to honor primary group (%s) for %s, group remains %s, error was: %s' % ( desired_group,
                                                                                                          path,
                                                                                                          current_group,
                                                                                                          e ) )



class DiskObjectStoreConfig:
    def __init__(self, job_work, new_file_path):
        self.object_store_check_old_style = False
        self.job_working_directory = job_work
        self.new_file_path = new_file_path
        self.umask = 077

class DiskObjectStore(ObjectStore):
    """
    Standard Galaxy object store, stores objects in files under a specific
    directory on disk.

    >>> from galaxy.util.bunch import Bunch
    >>> import tempfile
    >>> file_path=tempfile.mkdtemp()
    >>> obj = Bunch(id=1)
    >>> s = DiskObjectStore(Bunch(umask=077, job_working_directory=file_path, new_file_path=file_path, object_store_check_old_style=False), file_path=file_path)
    >>> s.create(obj)
    >>> s.exists(obj)
    True
    >>> assert s.get_filename(obj) == file_path + '/000/dataset_1.dat'
    """
    def __init__(self, config, config_xml=None, file_path=None, extra_dirs=None):
        super(DiskObjectStore, self).__init__(config, config_xml=None, file_path=file_path, extra_dirs=extra_dirs)
        self.file_path = file_path or config.file_path
        self.config = config
        self.check_old_style = config.object_store_check_old_style
        self.extra_dirs['job_work'] = config.job_working_directory
        self.extra_dirs['temp'] = config.new_file_path
        #The new config_xml overrides universe settings.
        if config_xml is not None:
            for e in config_xml:
                if e.tag == 'files_dir':
                    self.file_path = e.get('path')
                else:
                    self.extra_dirs[e.tag] = e.get('path')
        if extra_dirs is not None:
            self.extra_dirs.update( extra_dirs )

    def _get_filename(self, obj, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None):
        """Class method that returns the absolute path for the file corresponding
        to the `obj`.id regardless of whether the file exists.
        """
        path = self._construct_path(obj, base_dir=base_dir, dir_only=dir_only, extra_dir=extra_dir, extra_dir_at_root=extra_dir_at_root, alt_name=alt_name, old_style=True)
        # For backward compatibility, check the old style root path first; otherwise,
        # construct hashed path
        if not os.path.exists(path):
            return self._construct_path(obj, base_dir=base_dir, dir_only=dir_only, extra_dir=extra_dir, extra_dir_at_root=extra_dir_at_root, alt_name=alt_name)

    # TODO: rename to _disk_path or something like that to avoid conflicts with children that'll use the local_extra_dirs decorator, e.g. S3
    def _construct_path(self, obj, old_style=False, base_dir=None, dir_only=False, extra_dir=None, extra_dir_at_root=False, alt_name=None, **kwargs):
        """ Construct the expected absolute path for accessing the object
            identified by `obj`.id.

        :type base_dir: string
        :param base_dir: A key in self.extra_dirs corresponding to the base
                         directory in which this object should be created, or
                         None to specify the default directory.

        :type dir_only: bool
        :param dir_only: If True, check only the path where the file
                         identified by `obj` should be located, not the
                         dataset itself. This option applies to `extra_dir`
                         argument as well.

        :type extra_dir: string
        :param extra_dir: Append the value of this parameter to the expected path
                          used to access the object identified by `obj`
                          (e.g., /files/000/<extra_dir>/dataset_10.dat).

        :type alt_name: string
        :param alt_name: Use this name as the alternative name for the returned
                         dataset rather than the default.

        :type old_style: bool
        param old_style: This option is used for backward compatibility. If True
                         the composed directory structure does not include a hash id
                         (e.g., /files/dataset_10.dat (old) vs. /files/000/dataset_10.dat (new))
        """
        base = self.extra_dirs.get(base_dir, self.file_path)
        if old_style:
            if extra_dir is not None:
                path = os.path.join(base, extra_dir)
            else:
                path = base
        else:
            # Construct hashed path
            rel_path = os.path.join(*directory_hash_id(obj.uuid))
            # Optionally append extra_dir
            if extra_dir is not None:
                if extra_dir_at_root:
                    rel_path = os.path.join(extra_dir, rel_path)
                else:
                    rel_path = os.path.join(rel_path, extra_dir)
            path = os.path.join(base, rel_path)
        if not dir_only:
            path = os.path.join(path, alt_name if alt_name else "dataset_%s.dat" % obj.uuid)
        return os.path.abspath(path)

    def exists(self, obj, **kwargs):
        if self.check_old_style:
            path = self._construct_path(obj, old_style=True, **kwargs)
            # For backward compatibility, check root path first; otherwise, construct
            # and check hashed path
            if os.path.exists(path):
                return True
        return os.path.exists(self._construct_path(obj, **kwargs))

    def create(self, obj, **kwargs):
        if not self.exists(obj, **kwargs):
            path = self._construct_path(obj, **kwargs)
            dir_only = kwargs.get('dir_only', False)
            # Create directory if it does not exist
            dir = path if dir_only else os.path.dirname(path)
            if not os.path.exists(dir):
                os.makedirs(dir)
            # Create the file if it does not exist
            if not dir_only:
                open(path, 'w').close()  # Should be rb?
                umask_fix_perms(path, self.config.umask, 0666)

    def empty(self, obj, **kwargs):
        return os.path.getsize(self.get_filename(obj, **kwargs)) == 0

    def size(self, obj, **kwargs):
        if self.exists(obj, **kwargs):
            try:
                return os.path.getsize(self.get_filename(obj, **kwargs))
            except OSError:
                return 0
        else:
            return 0

    def delete(self, obj, entire_dir=False, **kwargs):
        path = self.get_filename(obj, **kwargs)
        extra_dir = kwargs.get('extra_dir', None)
        try:
            if entire_dir and extra_dir:
                shutil.rmtree(path)
                return True
            if self.exists(obj, **kwargs):
                os.remove(path)
                return True
        except OSError, ex:
            log.critical('%s delete error %s' % (self._get_filename(obj, **kwargs), ex))
        return False

    def get_data(self, obj, start=0, count=-1, **kwargs):
        data_file = open(self.get_filename(obj, **kwargs), 'r')  # Should be rb?
        data_file.seek(start)
        content = data_file.read(count)
        data_file.close()
        return content

    def get_filename(self, obj, **kwargs):
        if self.check_old_style:
            path = self._construct_path(obj, old_style=True, **kwargs)
            # For backward compatibility, check root path first; otherwise, construct
            # and return hashed path
            if os.path.exists(path):
                return path
        return self._construct_path(obj, **kwargs)

    def update_from_file(self, obj, file_name=None, create=False, **kwargs):
        """ `create` parameter is not used in this implementation """
        preserve_symlinks = kwargs.pop( 'preserve_symlinks', False )
        #FIXME: symlinks and the object store model may not play well together
        #these should be handled better, e.g. registering the symlink'd file as an object
        if create:
            self.create(obj, **kwargs)
        if file_name and self.exists(obj, **kwargs):
            try:
                if preserve_symlinks and os.path.islink( file_name ):
                    force_symlink( os.readlink( file_name ), self.get_filename( obj, **kwargs ) )
                else:
                    shutil.copy( file_name, self.get_filename( obj, **kwargs ) )
            except IOError, ex:
                log.critical('Error copying %s to %s: %s' % (file_name,
                    self._get_filename(obj, **kwargs), ex))
                raise ex

    def get_object_url(self, obj, **kwargs):
        return None

    def get_store_usage_percent(self):
        st = os.statvfs(self.file_path)
        return ( float( st.f_blocks - st.f_bavail ) / st.f_blocks ) * 100
