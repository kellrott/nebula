
import os
import stat

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
def force_symlink( source, link_name ):
    try:
        os.symlink( source, link_name )
    except OSError, e:
        if e.errno == errno.EEXIST:
            os.remove( link_name )
            os.symlink( source, link_name )
        else:
            raise e
