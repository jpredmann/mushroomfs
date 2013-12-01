import math
import time_uuid    #Used to generate unique chunk ids/keys
import os           #Used to write data out
import time         #Used to create timestamp for archiving deleted files
import operator     #Used in a Master 'tester method' (Server.dump_metadata())
import sys
import argparse
import signal
import re
import getopt
import errno
import logging
import pickle
import base64
import stat
import posix
import netifaces


logging.basicConfig( filename='mushroom_server.log', level=logging.DEBUG )

try:
    import Pyro.core, Pyro.naming
except:
    print >> sys.stderr, """
error: Pyro framework doesn't seem to be correctly installed!

Follow the instruction in the README file to install it, or go the Pyro
webpage http://pyro.sourceforge.net.
"""
    sys.exit(1)


###########################
###   MUSHROOM MASTER   ###
###########################

class MushroomMaster(Pyro.core.ObjBase):
    
    
    ############################
    ### Class Initialization ###
    ############################

    def __init__(self, root):
        logging.debug( '__INIT__' )
        
        self.root = os.path.abspath( root ) + '/'
        os.chdir( self.root )
        #self.chunksize = 1048576        # Max size of chunks in bytes (1MB)
        self.chunksize = 4096
        self.file_table = {}            # Look-up table to map from file paths to chunk ids
        self.chunk_table = {}           # Look-up table to map chunk id to chunk server
        self.chunk_server_table = {}    # Look-up table to map chunk servers to chunks held
        self.chunk_servers = ['MushroomChunkOne', 'MushroomChunkTwo', 'MushroomChunkThree', 'MushroomChunkFour' ]
        self.init_chunk_server_table()

        Pyro.core.ObjBase.__init__( self )
   
    
    ###########################################
    ### Routine: init_chunk_server_table    ###
    ###                                     ###
    ### Used by: N/A                        ###
    ###########################################
    
    # Make an dictionary of empty lists where keys are chunk servers and values are
    # a list of chunk ids stored on that chunk server
    def init_chunk_server_table( self ):
        logging.debug( 'INIT_CHUNK_SERVER_TABLE' )
    
        for server in self.chunk_servers:
            self.chunk_server_table[ server ] = []


    #####################################
    ### Routine: get_chunk_servers    ###
    ###                               ###
    ### Used by: Client.write_chunks  ###
    #####################################
            
    # Returns a list of chunk servers considered by the master server to be
    # currently available.  This list get updated by the master server when 
    # Zookeeper reports that a chunk server is no longer available.
    def get_chunk_servers( self ):
        logging.debug( 'GET_CHUNK_SERVERS' )
        self.chunk_servers.append( self.chunk_servers.pop(0) )
        return self.chunk_servers

    def get_chunk_size( self ):
        logging.debug( 'GET_CHUNK_SIZE' )
        
        return self.chunksize
                
                
    ####################################
    ### Routine: generte_chunk_ids   ###
    ###                              ###
    ### Used by: Client.MF.write     ###
    ####################################

    # Returns a list of chunk ids.  Calls an internal chunk allocation method to
    # perform the 'house keeping' tasks with the meta-data tables.
    def generate_chunk_ids(self, file_descriptor, path, num_chunks, file_size ): # return ordered chunkuuid list
        logging.debug( 'GENERATE_CHUNK_IDS' )
    
        chunk_ids = []                    # List to hold chunk ids
        # Iterate over the number of chunks the file has been split into
        for i in range(0, num_chunks):
        
            # Generate a new UUID for the current chunk
            chunkuuid = time_uuid.TimeUUID.with_timestamp( time.time() )
            path_string = base64.urlsafe_b64encode( path )
            # add the new chunk id--consisting of a UUID, path pair, to the chunk id list
            chunk_ids.append( (chunkuuid, path_string) )
        
        # Adds file path to the file table, if it was not present already
        # stores, potentially over-writing, a list of chunk ids, where those chunks
        # compose the file.
        #self.file_table[path] = chunk_ids
        #with open( path, 'wb' ) as file:
            #pickle.dump( chunk_ids, file )
        
        os.write( file_descriptor, "updating" )
        self.file_table[ path ].extend( chunk_ids )
        self.file_table[ path + 'size' ] = self.file_table[ path +'size' ] + file_size
        return chunk_ids
                
                
    ####################################
    ### Subroutine: register_chunks  ###
    ###                              ###
    ### Used by: client.MF.write     ###
    ####################################

    # Internal house keeping method to update meta-data tables, returns a list of chunk
    # ids back to the allocating method that called it.
    def register_chunks(self, actual_writes, path ):
        logging.debug( 'REGISTER_CHUNKS' )
        chunk_ids = actual_writes.keys() 
        # Iterate over the number of chunks the file has been split into
        for id in chunk_ids:
            # Add entry into the chunk table for the new UUID containing primary server
            # for that chunk (pre-replication).
            uuid = id[0]
            chunk_location = actual_writes[ id ]
            self.chunk_table[ uuid ] = [ chunk_location ]
            # Append to the entry in the chunk server table the chunk id that is now held
            # on that chunk server.
            self.chunk_server_table[ chunk_location ].append( id ) 

    def deregister_chunks( self, path, delete_dict ):
        
        for chunk_server in delete_dict.keys():
            delete_list = delete_dict[ chunk_server ]
            chunk_ids_list = self.chunk_server_table[ chunk_server ]
            self.chunk_server_table[ chunk_server ] = [ chunk for chunk in chunk_ids_list if chunk not in delete_list ]

        chunk_ids_list = self.file_table[ path ]

        for chunk_id in chunk_ids_list:
            del self.chunk_table[ chunk_id ]

        del self.file_table[ path ]
        del self.file_table[ path + 'size' ]
        os.unlink( self.root + path )
                
    #######################################
    ### Routine: alloc_append           ###
    ###                                 ###
    ### Used by: N/A                    ###
    #######################################
        
    # Allocation method for appending to a file
    def alloc_append(self, path, num_append_chunks): # append chunks
        logging.debug( 'ALLOC_APPEND' )
        
        # Get chunk ids for existing chunks in file
        chunk_ids = self.file_table[path]
        # Call to house keeping subroutine to get chunk ids for chunks being appended
        append_chunk_ids = self.alloc_chunks(num_append_chunks, path)
        # Append chunk ids to list of existing chunks
        chunk_ids.extend(append_chunk_ids)
        
        return append_chunk_ids


    #######################
    ### Routine: ping   ###
    ###                 ###
    ### TESTING ONLY    ###
    ### #TODO delete    ###
    #######################
    def ping(self):
        logging.debug('PING-PONG')
        return "pong"            
                
    ######################################
    ### Routine: get_chunkloc          ###
    ###                                ###
    ### Used by: Client.MF.read        ###
    ######################################

    # Get the list of chunk servers that hold the given chunk
    def get_chunkloc(self, chunk_id):
        logging.debug( 'GET_CHUNKLOC' )
        return self.chunk_table[chunk_id]
                
                
    ##############################################
    ### Routine: get_chunk_ids                 ###
    ###                                        ###
    ### Used by: Client.rename, Client.MF.read ###
    ##############################################

    # Get the list of ids of the chunks that compose the given file
    def get_chunk_ids(self, path):
        logging.debug( 'GET_CHUNK_IDS' )
        return self.file_table[ path ]
                
                
    ###############################################
    ### Routine: exists                         ###
    ###                                         ###
    ### Used by: Client.rename, Client.MF.write ###
    ###############################################

    # Determine if the file already exists
    def exists(self, path):
        logging.debug( 'EXISTS' )
    
        return os.path.exists( path ) 
                
                
    ###################################
    ### Routine: delete             ###
    ###                             ###
    ### Used by: N/A                ###
    ###################################
    
    def delete(self, path): # rename for later garbage collection
        logging.debug( 'DELETE' )
    
        chunk_ids = self.file_table[path]
        
        for id in chunk_ids:
            for server in self.chunk_table[ id ]:
                self.chunk_server_table[server].remove( id )
            
            del self.chunk_table[id]
        
        del self.file_table[path]
    
    
    ###########################################
    ### Routine: register_chunk_server      ###
    ###                                     ###
    ### Used by: N/A                        ###
    ###########################################

    def register_chunk_server(self, chunkserver_name):
        logging.debug( 'REGISTER_CHUNK_SERVER' )
        self.chunk_servers.append(chunkserver_name)   

    
    """
    FILE ROUTINES
    """
    
    ####################################
    ### Routine: ftruncate           ###
    ###                              ###
    ### Used by: Client.MF.ftruncate ###
    ####################################
    
    def ftruncate( self, file_descriptor, length ):
        logging.debug( 'FTRUNCATE' )
        
        try:
            op_result = os.ftruncate( file_descriptor, length )
        except:
            logging.debug( 'EXCEPTION FTRUNCATE' )
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: fgetattr            ###
    ###                              ###
    ### Used by: Client.MF.fgetattr  ###
    ####################################
    
    def fgetattr( self, file_descriptor, path ):
        logging.debug( 'FGETATTR' )
        
        try:
            op_result = os.fstat( file_descriptor )
            file_size = self.file_table[ path + 'size' ]
            stats_list = list( op_result )
            stats_list[ stat.ST_SIZE ] = file_size
            op_result = posix.stat_result( stats_list )
        except:
            logging.debug( 'EXCEPTION FTRUNCATE' )
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: flush               ###
    ###                              ###
    ### Used by: Client.MF.flush     ###
    ####################################
    
    def flush( self, file_descriptor ):
        logging.debug( 'FLUSH' )
        
        try:
            op_result = os.close( os.dup( file_descriptor ) )
        except:
            logging.debug( 'EXCEPTION FLUSH' )
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: fsync               ###
    ###                              ###
    ### Used by: Client.MF.fsync     ###
    ####################################
    
    # TODO: determine what should be returned
    def fsync( self, file_descriptor, isfsyncfile ):
        logging.debug( 'FSYNC' )
        
        try:
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync( file_descriptor )
            else:
                os.fsync( file_descriptor )
        except:
            logging.debug( 'EXCEPTION FSYNC' )
            return -errno.EACCES
    
    
    """
    FILE SYSTEM ROUTINES
    """
            
    # Need to implement a replace chunk server method that can be called by Zookeeper
    # the call will provide the ip and port of a replacement chunk server
    
    #################################
    ### Routine: statfs           ###
    ###                           ###
    ### Used by: Client.statfs    ###
    #################################
    
    def statfs( self ):
        logging.debug( 'STATFS' )
    
        try:
            op_result =  os.statvfs( self.root )
        except:
            logging.debug( 'EXCEPTION STATFS' )
            op_result = -errno.EACCES
            
        return op_result

    
    #################################
    ### Routine: getattr          ###
    ###                           ###
    ### Used by: Client.getattr   ###
    #################################

    def getattr( self, path ):
        logging.debug( 'GETATTR' )
    
        try:
            op_result = os.lstat( self.root + path[1:] )
            if path in self.file_table.keys():
                file_size = self.file_table[ path + 'size' ]
                stats_list = list( op_result )
                stats_list[ stat.ST_SIZE ] = file_size
                op_result = posix.stat_result( stats_list )
                
        except:
            logging.debug( 'EXCEPTION GETATTR' )
            op_result = -errno.ENOENT
            
        return op_result
    
    
    #################################
    ### Routine: readlink         ###
    ###                           ###
    ### Used by: Client.readlink  ###
    #################################

    def readlink( self, path ):
        logging.debug( 'READLINK' )
    
        try:
            op_result =  os.readlink( self.root + path )
        except:
            logging.debug( 'EXCEPTION READLINK' )
            op_result = -errno.ENOENT
            
        return op_result
    
    
    #################################
    ### Routine: readdir          ###
    ###                           ###
    ### Used by: Client.readdir   ###
    #################################

    def readdir( self, path ):
        logging.debug( 'READDIR' )
    
        try:
            op_result = os.listdir( self.root + path )
        except:
            logging.debug( 'READDIR' )
            op_result = -errno.EBADF
            
        return op_result
    
    
    #################################
    ### Routine: open             ###
    ###                           ###
    ### Used by: Client.open      ###
    #################################

    def open( self, path, flags, mode ):
        logging.debug( 'OPEN' )
        
        key = path + 'size' 
        # if successful the op_result holds the file descriptor
        try:
            if mode:
                op_result = os.open( self.root + path[1:], os.O_CREAT|os.O_RDWR, mode[0] )
                if key not in self.file_table.keys():
                    self.file_table[ path ] = []
                    self.file_table[ path + 'size' ] = 0
            else:
                op_result = os.open( self.root + path[1:], os.O_CREAT|os.O_RDWR )
                if key not in self.file_table.keys():
                    self.file_table[ path ] = []
                    self.file_table[ path + 'size' ] = 0
                
        # if not successful op_result holds the error code        
        except:
            logging.debug( 'EXCEPTION OPEN' )
            op_result = -errno.ENOENT
        return op_result
    
    
    #################################
    ### Routine: release          ###
    ###                           ###
    ### Used by: Client.release   ###
    #################################

    # TODO: Determine what if anything should be returned here
    def release( self, file_descriptor, flags ):
        logging.debug( 'RELEASE' )
    
        try:
            if file_desciptor > 0:
                os.close( file_descriptor )
                op_result = True
        except:
            logging.debug( 'EXCEPTION RELEASE' )
            op_result =  -errno.ENOSYS
            
        return op_result


    ####################################
    ### Routine: truncate            ###
    ###                              ###
    ### Used by: Client.truncate     ###
    ####################################

    # TODO: return issues
    def truncate( self, path, length ):
        logging.debug( 'TRUNCATE' )
    
        try:
            file = open( self.root + path, 'ab' )
            file.truncate( length )
            file.close()
        except:
            logging.debug( 'EXCEPTION TRUNCATE' )
            return -errno.EACCES


    #################################
    ### Routine: mkdir            ###
    ###                           ###
    ### Used by: Client.mkdir     ###
    #################################

    def mkdir(self, path, mode):
        logging.debug( 'MKDIR' )
    
        try:
            op_result = os.mkdir( self.root + path, mode )
        except:
            logging.debug( 'EXCEPTION MKDIR' )
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: rmdir              ###
    ###                             ###
    ### Used by: Client.rmdir       ###
    ###################################

    def rmdir( self, path ):
        logging.debug( 'RMDIR' )
    
        try:
            op_result = os.rmdir( self.root + path )
        except:
            logging.debug( 'EXCEPTION RMDIR' )
            op_result = -errno.EACCES
            
        return op_result


    ####################################
    ### Routine: symlink             ###
    ###                              ###
    ### Used by: Client.symlink      ###
    ####################################

    def symlink( self, source_path, target_path ):
        logging.debug( 'SYMLINK' )
    
        try:
            op_result = os.symlink( source_path, self.root + target_path )
        except:
            logging.debug( 'EXCEPTION SYMLINK' )
            op_result = -errno.EACCES
            
        return op_result


    ##################################
    ### Routine: link              ###
    ###                            ###
    ### Used by: Client.link       ###
    ##################################

    def link( self, source_path, target_path ):
        logging.debug( 'LINK' )
    
        try:
            op_result = os.link( source_path, self.root + target_path )
        except:
            logging.debug( 'EXCEPTION LINK' )
            op_result = -errno.EACCES
            
        return op_result
        

    ####################################
    ### Routine: unlink              ###
    ###                              ###
    ### Used by: Client.unlink       ###
    ####################################

    def unlink( self, path ):
        logging.debug( 'UNLINK' )
    
        try:
            op_result = os.unlink( self.root + path )
        except:
            logging.debug( 'EXCEPTION UNLINK' )
            op_result = -errno.EACCES
            
        return op_result
        

    ####################################
    ### Routine: rename              ###
    ###                              ###
    ### Used by: Client.rename       ###
    ####################################

    def rename( self, source_path, target_path ):
        logging.debug( 'RENAME' )
    
        try:
            op_result = os.rename( self.root + source_path, self.root + target_path )
        except:
            logging.debug( 'EXCEPTION RENAME' )
            op_result = -errno.EACCES
            
        return op_result
        

    ###################################
    ### Routine: chmod              ###
    ###                             ###
    ### Used by: Client.chmod       ###
    ###################################

    def chmod( self, path, mode ):
        logging.debug( 'CHMOD' )
    
        try:
            op_result = os.chmod( self.root + path, mode )
        except:
            logging.debug( 'CHMOD' )
            op_result = -errno.EACCES
            
        return op_result
        
        
    ###################################
    ### Routine: chown              ###
    ###                             ###
    ### Used by: Client.chown       ###
    ###################################

    def chown(self, path, user, group):
        logging.debug( 'CHOWN' )
        try:
            return os.chown(self.root + path, user, group)
        except:
            logging.debug( 'EXCEPTION CHOWN' )
            return -errno.EACCES


    ###################################
    ### Routine: mknod              ###
    ###                             ###
    ### Used by: Client.mknod       ###
    ###################################

    def mknod( self, path, mode, device ):
        logging.debug( 'MKNOD' )
    
        try:
            op_result = os.mknod( self.root + path, mode, device )
        except:
            logging.debug( 'EXCEPTION MKNOD' )
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: utime              ###
    ###                             ###
    ### Used by: Client.utime       ###
    ###################################

    def utime( self, path, times ):
        logging.debug( 'UTIME' )
    
        try:
            op_result = os.utime( self.root + path, times )
        except:
            logging.debug( 'EXCEPTION UTIME' )
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: access             ###
    ###                             ###
    ### Used by: Client.access      ###
    ###################################

    # TODO: This looks incorrect, look into it
    def access( self, path, mode ):
        logging.debug( 'ACCESS' )
    
        try:
            if not os.access( self.root + path, mode ):
                raise
        except:
            logging.debug( 'EXCEPTION ACCESS' )
            return -errno.EACCES


#################
###   MAIN    ###
#################
    
# Main program.
def main():
    logging.debug( 'MAIN' )

    # Set default options.
    mountpoint = "~/server"

    parser = argparse.ArgumentParser()

    parser.add_argument( "mountpoint", help="A directory or mount point must be specified" )
    parser.add_argument( '-v', '--version', action='store_true', help="Prints version information for the chunk server" )

    args = parser.parse_args()
    mountpoint = args.mountpoint

    # Instantiate chunk server
    try:
        master_server = MushroomMaster( mountpoint )
    except:
        print >> sys.stderr, "Error: could not mount " + mountpoint
        sys.exit(1)

    # Go in deamon-mode.
    pid = 0
    
    try:
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

        #find the nameserver
        ns=Pyro.naming.NameServerLocator().getNS(host='192.168.1.22')
        
        ip = netifaces.ifaddresses('eth0')[2][0]['addr']
        daemon = Pyro.core.Daemon('PYRO',ip)
            
        daemon.useNameServer(ns)
        uri = daemon.connect(master_server, 'MushroomFS')
        
        try:
            # Start the daemon.
            daemon.requestLoop()
        except:
            # Shutdown the daemon.
            daemon.shutdown(True)

if __name__ == '__main__':
    main()

