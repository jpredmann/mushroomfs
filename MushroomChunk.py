#!/usr/bin/env python

import sys        # Used to generate standard error messages and exit
import os         # Used to access the underlying file system
import errno      # Used to return error codes to FUSE
import signal     # Used to process system signals for the daemon
import argparse   # Used to process command line arguments

# Pyro is used for remote procedure call
try:
    import Pyro.core, Pyro.naming
except:
    print >> sys.stderr, """
    error: Pyro framework doesn't seem to be correctly installed!

    Follow the instruction in the README file to install it, or go the Pyro
    webpage http://pyro.sourceforge.net.
    """
    sys.exit(1)


# Used to aquire this machines ip address for name server registration
try:
    import netifaces 
except:
    print >> sys.stderr, """
    error: Netifaces framework doesn't seem to be correctly installed!

    Follow the instruction in the README file to install it, or use pip install netifaces
    """
    sys.exit(1)




# The chunk server class.
class MushroomChunk( Pyro.core.ObjBase ):
"""
Class implementing the data chunk server
"""


    def __init__( self, root ):
    """
    Constructor
    """

        # Set the directory in the server that will be used to store the data chunks
        self.root = os.path.abspath( root ) + '/'
        # Change to the directory set above
        os.chdir( self.root )
        # Register the chunk server object with Pyro for RPC
        Pyro.core.ObjBase.__init__( self )

    # END __init__


    def getattr( self, path ):
    """
    getattr method is used by FUSE to get file system attributes
    """
    
        try:
            op_result = os.lstat( self.root + path )
        except:
            op_result = -errno.ENOENT
            
        return op_result

    # END getattr
            

    def read( self, path ):
    """
    read method returns a given data chunk
    """
    
        try:
            with open(path, "rb") as file:
                op_result = file.read()
        except:
            op_result = -errno.EACCES
            
        return op_result
    
    # END read


    def write( self, data, path ):
    """
    write method creates/overwrites a data chunk file
    """
    
        try:
            with open( self.root + path, 'w' ) as file:
                file.write( data )
            op_result = len( data )
        except:
            op_result = -errno.EACCES
        
        return op_result

    # END write


    def rename( self, source_list, target_list ):
    """
    rename method is used to update the names of data chunks
    this occures in the case that a file name changes or the file path
    changes.  Data chunk file names embed path and file name of the
    primary file.
    """

        try:
            for source_chunk_id, target_chunk_id in zip( source_list, target_list ):
                # construct the original file namne for the data chunk from the chunk uuid and primary file's path
                source_path = str( source_chunk_id[0] ) + "--" + source_chunk_id[1]
                # construct the new file namne for the data chunk from the chunk uuid and primary file's path
                target_path = str( target_chunk_id[0] ) + "--" + target_chunk_id[1]
                op_result = os.rename( self.root + source_path, self.root + target_path )
        except:
            op_result = -errno.EACCES
            
        return op_result

    # END rename


    def utime( self, path, times ):
    """
    utime method updates the data chunk files access time
    FOR FUTURE FEATURES
    """

        try:
            op_result = os.utime( self.root + path, times )
        except:
            op_result = -errno.EACCES
            
        return op_result

    # END utime


    def access( self, path, mode ):
    """
    access method used to determin if a file is accessible
    """

        try:
            if not os.access( self.root + path, mode ):
                raise
        except:
            return -errno.EACCES

    # END access

    def delete( self, delete_list ):
    """
    delete method used to delete a specified data chunk 
    The delete method is used to clear out all data chunks on the server
    associated with a primary file.
    """    

        try:
        
            for chunk_name in delete_list:
                os.unlink( self.root + chunk_name )
        except:
            return -errno.EACCES

    # END delete


    def readdir( self ):
    """
    readdir returns the directory listing of data chunk files for 
    master server failure recovery
    """

        file_size_list = []                  # stores the sizes of the data chunk files on this server
        dir_list = os.listdir( self.root )   # stores the file names of the data chunk files on this server
        recovery_data = {}                   # dictionary containing the above lists

        # strip out the special directories '.' and '..'
        stripped_dir_list = [ item for item in dir_list if item[0] != '.' ]

        # loop to get data chunk file sizes
        for item in stripped_dir_list:
            file_size = os.path.getsize( self.root + item )
            file_size_list.append( file_size )

        # store lists in dictionary
        recovery_data[ 'size'] = file_size_list
        recovery_data[ 'files' ] = stripped_dir_list

        return recovery_data

    # END readdir
        

# Main program.
def main():

    # Default mount point ( convenient for our mounting )
    mount_point = '/home/groupd/chunk'

    # parser for command line arguments to the chunk server
    parser = argparse.ArgumentParser()

    # get the mount point and the Pyro name server
    parser.add_argument( 'mount_point', help="A directory or mount point must be specified" )
    parser.add_argument( 'server_name', help="The name used by Pyro to identify this chunk server" )

    # set the mount point and name server from the command line arguments
    args = parser.parse_args()
    mount_point = args.mount_point
    server_name = args.server_name

    # Instantiate chunk server
    try:
        chunk_server = MushroomChunk( mount_point )
    except:
        print >> sys.stderr, "Error: could not mount " + mount_point
        sys.exit(1)

    # Go in deamon-mode.
    pid = 0
    
    try:
        # fork the chunk server into a new process
        pid = os.fork()
    except OSError, e:
        raise Exception, "%s [%d]" % (e.strerror, e.errno)
        
    if pid:
        # Run the daemon in background.
        os._exit(0)
    else:
        os.umask(0)
        # Call the same handler both for SIGHUP and SIGINT.
        signal.signal(signal.SIGHUP, signal.default_int_handler)
        
        # Initialize the Pyro RPC object.
        Pyro.core.initServer(banner=0)

        # get Pyro name server
        ns=Pyro.naming.NameServerLocator().getNS(host='137.30.122.76')

        # get the ip address of this chunk server       
        ip = netifaces.ifaddresses('en0')[2][0]['addr']

        # create the Pyro daemon
        daemon = Pyro.core.Daemon('PYRO', ip)

        # register the name server with the daemon
        daemon.useNameServer(ns)
        
        # WARNING: This is actually important
        # the uri variable is unused at this time, however
        # the binding call is needed for RPC    
        uri = daemon.connect( chunk_server, server_name )
        
        try:
            # Start the daemon.
            daemon.requestLoop()
        except:
            # Shutdown the daemon.
            daemon.shutdown(True)

if __name__ == '__main__':
    main()

