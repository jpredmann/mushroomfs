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


################################
### MUSHROOM CERT VALIDATOR  ###
################################

# Certificate validator class.
class MushroomCertValidator(Pyro.protocol.BasicSSLValidator):
    
    
    ############################
    ### Class Initialization ###
    ############################
    def __init__(self):
        if os.path.isfile('authorized-clients'):
            self.cfgfile = 'authorized-clients'
        else:
            sys.exit(1)

        self.clients = self.clients_regexp = {}
        
        try:
            # Read authorized-clients file.
            f = open(self.cfgfile, 'r')
            for line in f:
                # Strip trailing end-line
                line = line.rstrip('\n')
                # Skip inline comments
                line = re.sub('#.*', '', line)
                # Strip spaces
                line = line.strip()
                # Skip empty lines
                if line == '':
                    continue
                # Adding a new entry to the hash of authorized clients
                regexp = re.match(r'^regexp:(.*)', line)
                if regexp is None:
                    self.clients[line] = True
                else:
                    self.clients_regexp[regexp.group(1)] = True
            f.close()
        except Exception, e:
            raise e

        Pyro.protocol.BasicSSLValidator.__init__(self)
    
    ###############################
    ### Routine: MCV.authorize  ###
    ###############################

    def authorize(self, subject):
        return (1, 0)

    ###############################
    ### Routine: MCV.deny       ###
    ###############################

    def deny(self, subject, code):
        return (0, code)
            
    #####################################
    ### Routine: MCV.checkCertificate ###
    #####################################

    def checkCertificate(self, cert):
    
        # Client must have a valid certificate
        if cert is None:
            return self.deny('NULL', 3)
            
        # Subject must match one of the subjects specified in the
        # authorized-clients file
        subject = str(cert.get_subject())
        
        if self.clients.has_key(subject):
            return self.authorize(subject)
        else:
            # Check also if the subject matches one of the defined
            # regular expressions
            for m in self.clients_regexp.keys():
                if re.match(m, subject):
                    return self.authorize(subject)
                    
            return self.deny(subject, 4)



###########################
###   MUSHROOM MASTER   ###
###########################

class MushroomMaster(Pyro.core.ObjBase):
    
    
    ############################
    ### Class Initialization ###
    ############################

    def __init__(self, root):
        
        self.root = os.path.abspath( root ) + '/'
        os.chdir( self.root )
        #self.chunksize = 1048576        # Max size of chunks in bytes (1MB)
        self.chunksize = 1000
        self.chunkrobin = 0             # Index of the next chunk server to use
        self.file_table = {}            # Look-up table to map from file paths to chunk ids
        self.chunk_table = {}           # Look-up table to map chunk id to chunk server
        self.chunk_server_table = {}    # Look-up table to map chunk servers to chunks held
        #self.chunk_servers = [ ( '0.0.0.0', 3637 ), ('0.0.0.0', 3638) ]         # List of registered chunk servers
        self.chunk_servers = ['MushroomChunkOne', 'MushroomChunkTwo' ]
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
        logging.debug( 'in get chunk servers')
        self.chunk_servers.append( self.chunk_servers.pop(0) )
        logging.debug( self.chunk_servers )
        return self.chunk_servers

    def get_chunk_size( self ):
        
        return self.chunksize

    #################################
    ### write_metafile             ###
    ### USed by: generate_chunkids ###
    #################################
    def write_metafile(self, path, chunk_string):
        f = open(self.root + path, 'w')
        f.write(chunk_string)
        f.flush()
        f.close()
        #import subprocess
        #subprocess.call(['echo', " \" " + chunk_string + " \" ", '>', self.root+path[1:]])        
                
                
    ####################################
    ### Routine: alloc               ###
    ###                              ###
    ### Used by: Client.MF.write     ###
    ####################################

    # Returns a list of chunk ids.  Calls an internal chunk allocation method to
    # perform the 'house keeping' tasks with the meta-data tables.
    def generate_chunk_ids(self, file_descriptor, path, num_chunks): # return ordered chunkuuid list
    
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
        
        #chunk_string = pickle.dumps( chunk_ids )
        #os.write( file_descriptor, chunk_string )
        self.file_table[ path_string ] = chunk_ids
        return chunk_ids
                
                
    #################################
    ### Subroutine: alloc_chunks  ###
    ###                           ###
    ### Used by: alloc            ###
    #################################

    # Internal house keeping method to update meta-data tables, returns a list of chunk
    # ids back to the allocating method that called it.
    def register_chunks(self, actual_writes ):
        logging.debug( 'In register_chunks' ) 
        chunk_ids = actual_writes.keys() 
        logging.debug( 'chunk_ids' )
        logging.debug( chunk_ids )
        # Iterate over the number of chunks the file has been split into
        for id in chunk_ids:
            logging.debug( 'In for loop for reister_chunks' )
            logging.debug( id )
            # Add entry into the chunk table for the new UUID containing primary server
            # for that chunk (pre-replication).
            uuid = id[0]
            logging.debug( 'UUID in register_chunks' )
            logging.debug( uuid )
            logging.debug( 'id' )
            logging.debug( id )
            chunk_location = actual_writes[ id ]
            logging.debug( 'chunk_location' )
            logging.debug( chunk_location )
            self.chunk_table[ uuid ] = [ chunk_location ]
            logging.debug('CHUNK TABLES CONTENTS')
            logging.debug(self.chunk_table)
            logging.debug( 'set chunk table entry' )
            # Append to the entry in the chunk server table the chunk id that is now held
            # on that chunk server.
            self.chunk_server_table[ chunk_location ].append( id ) 
            logging.debug( 'set chunk server table' )
            logging.debug(self.chunk_server_table)
            
                
    #######################################
    ### Routine: alloc_append           ###
    ###                                 ###
    ### Used by: N/A                    ###
    #######################################
        
    # Allocation method for appending to a file
    def alloc_append(self, path, num_append_chunks): # append chunks
        
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
        logging.debug('CALL TO GET CHUNK LOCATIONS')
        logging.debug('the key from client is:')
        logging.debug(chunk_id)
        logging.debug('the value for the chunk is:')
        logging.debug(self.chunk_table[chunk_id])    
        return self.chunk_table[chunk_id]
                
                
    ##############################################
    ### Routine: get_chunk_ids                 ###
    ###                                        ###
    ### Used by: Client.rename, Client.MF.read ###
    ##############################################

    # Get the list of ids of the chunks that compose the given file
    def get_chunk_ids(self, file_descriptor, path):
        logging.debug( 'MADE IT TO GET CHUNK IDS' )
        logging.debug( self.root + path[1:] )
        #mode = os.fstat( file_descriptor )
        #logging.debug( 'Got mode in get chunk ids' )
        #logging.debug( mode )
        #file_size = mode[ stat.ST_SIZE ]
        #logging.debug( 'Got file size' )
        #logging.debug( file_size )
        #chunk_string = os.read( file_descriptor, file_size  )
        #logging.debug( 'Got chunk_ids string' )
        #logging.debug( chunk_string )
        #chunk_ids = pickle.loads( chunk_string )
        #logging.debug( 'Got chunk_ids' )
        #logging.debug( chunk_ids )
        return self.file_table[ path ]
                
                
    ###############################################
    ### Routine: exists                         ###
    ###                                         ###
    ### Used by: Client.rename, Client.MF.write ###
    ###############################################

    # Determine if the file already exists
    def exists(self, path):
    
        return os.path.exists( path ) 
                
                
    ###################################
    ### Routine: delete             ###
    ###                             ###
    ### Used by: N/A                ###
    ###################################
    
    def delete(self, path): # rename for later garbage collection
    
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

    #def register_chunk_server(self, ip_address, port_number):
        #self.chunk_servers.append( (ip_address, port_number) )
    def register_chunk_server(self, chunkserver_name):
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
        
        try:
            op_result = os.ftruncate( file_descriptor, length )
        except:
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: read                ###
    ###                              ###
    ### Used by: Client.MF.read      ###
    ####################################
    
    def read( self, file_descriptor, size, offset ):
        
        try:
            os.lseek( file_descriptor, offset, 0 )
            op_result = os.read( file_descriptor, size )
        except:
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: write               ###
    ###                              ###
    ### Used by: Client.MF.write     ###
    ####################################
    
    def write( self, file_descriptor, buffer, offset ):
        
        try:
            os.lseek( file_descriptor, offset, 0 )
            os.write( file_descriptor, buffer )
            op_result = len( buffer )
        except:
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: fgetattr            ###
    ###                              ###
    ### Used by: Client.MF.fgetattr  ###
    ####################################
    
    def fgetattr( self, file_descriptor ):
        
        try:
            op_result = os.fstat( file_descriptor )
        except:
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: flush               ###
    ###                              ###
    ### Used by: Client.MF.flush     ###
    ####################################
    
    def flush( self, file_descriptor ):
        
        try:
            op_result = os.close( os.dup( file_descriptor ) )
        except:
            op_result = -errno.EACCES
        
        return op_result
    
    
    ####################################
    ### Routine: fsync               ###
    ###                              ###
    ### Used by: Client.MF.fsync     ###
    ####################################
    
    # TODO: determine what should be returned
    def fsync( self, file_descriptor, isfsyncfile ):
        
        try:
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync( file_descriptor )
            else:
                os.fsync( file_descriptor )
        except:
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
    
        try:
            op_result =  os.statvfs( self.root )
        except:
            op_result = -errno.EACCES
            
        return op_result

    
    #################################
    ### Routine: getattr          ###
    ###                           ###
    ### Used by: Client.getattr   ###
    #################################

    def getattr( self, path ):
    
        try:
            #op_result = os.lstat( self.root + path )
            op_result = os.lstat( self.root + path[1:] )
            logging.debug( 'Path is' )
            logging.debug( self.root + path[1:] )
            logging.debug( op_result )
        except:
            op_result = -errno.ENOENT
            
        return op_result
    
    
    #################################
    ### Routine: readlink         ###
    ###                           ###
    ### Used by: Client.readlink  ###
    #################################

    def readlink( self, path ):
    
        try:
            op_result =  os.readlink( self.root + path )
        except:
            op_result = -errno.ENOENT
            
        return op_result
    
    
    #################################
    ### Routine: readdir          ###
    ###                           ###
    ### Used by: Client.readdir   ###
    #################################

    def readdir( self, path ):
    
        try:
            op_result = os.listdir( self.root + path )
        except:
            op_result = -errno.EBADF
            
        return op_result
    
    
    #################################
    ### Routine: open             ###
    ###                           ###
    ### Used by: Client.open      ###
    #################################

    def open( self, path, flags, mode ):
    
        # if successful the op_result holds the file descriptor
        try:
            if mode:
                #iop_result = os.open( self.root + path, flags, mode[0] )
                op_result = os.open( self.root + path, os.O_CREAT|os.O_RDWR, mode[0] )
                logging.debug( 'Flags' )
                logging.debug( flags )
                logging.debug( 'Just opened file with mode' )
                logging.debug( path )
                logging.debug( os.stat( self.root + path[1:] ) )
                logging.debug( self.root + path[1:] )
                logging.debug( 'Done opening file with mode' )
            else:
                op_result = os.open( self.root + path[1:], flags )
                logging.debug( path )
                logging.debug( 'Just opened file no mode' )
                logging.debug( self.root + path )
                logging.debug( os.stat( self.root + path ) )
                
        # if not successful op_result holds the error code        
        except:
            op_result = -errno.ENOENT
            
        return op_result
    
    
    #################################
    ### Routine: release          ###
    ###                           ###
    ### Used by: Client.release   ###
    #################################

    # TODO: Determine what if anything should be returned here
    def release( self, file_descriptor, flags ):
    
        try:
            if file_desciptor > 0:
                os.close( file_descriptor )
                op_result = True
        except:
            op_result =  -errno.ENOSYS
            
        return op_result


    ####################################
    ### Routine: truncate            ###
    ###                              ###
    ### Used by: Client.truncate     ###
    ####################################

    # TODO: return issues
    def truncate( self, path, length ):
    
        try:
            file = open( self.root + path, 'ab' )
            file.truncate( length )
            file.close()
        except:
            return -errno.EACCES


    #################################
    ### Routine: mkdir            ###
    ###                           ###
    ### Used by: Client.mkdir     ###
    #################################

    def mkdir(self, path, mode):
    
        try:
            op_result = os.mkdir( self.root + path, mode )
        except:
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: rmdir              ###
    ###                             ###
    ### Used by: Client.rmdir       ###
    ###################################

    def rmdir( self, path ):
    
        try:
            op_result = os.rmdir( self.root + path )
        except:
            op_result = -errno.EACCES
            
        return op_result


    ####################################
    ### Routine: symlink             ###
    ###                              ###
    ### Used by: Client.symlink      ###
    ####################################

    def symlink( self, source_path, target_path ):
    
        try:
            op_result = os.symlink( source_path, self.root + target_path )
        except:
            op_result = -errno.EACCES
            
        return op_result


    ##################################
    ### Routine: link              ###
    ###                            ###
    ### Used by: Client.link       ###
    ##################################

    def link( self, source_path, target_path ):
    
        try:
            op_result = os.link( source_path, self.root + target_path )
        except:
            op_result = -errno.EACCES
            
        return op_result
        

    ####################################
    ### Routine: unlink              ###
    ###                              ###
    ### Used by: Client.unlink       ###
    ####################################

    def unlink( self, path ):
    
        try:
            op_result = os.unlink( self.root + path )
        except:
            op_result = -errno.EACCES
            
        return op_result
        

    ####################################
    ### Routine: rename              ###
    ###                              ###
    ### Used by: Client.rename       ###
    ####################################

    def rename( self, source_path, target_path ):
    
        try:
            op_result = os.rename( self.root + source_path, self.root + target_path )
        except:
            op_result = -errno.EACCES
            
        return op_result
        

    ###################################
    ### Routine: chmod              ###
    ###                             ###
    ### Used by: Client.chmod       ###
    ###################################

    def chmod( self, path, mode ):
    
        try:
            op_result = os.chmod( self.root + path, mode )
        except:
            op_result = -errno.EACCES
            
        return op_result
        
        
    ###################################
    ### Routine: chown              ###
    ###                             ###
    ### Used by: Client.chown       ###
    ###################################

    def chown(self, path, user, group):
        try:
            return os.chown(self.root + path, user, group)
        except:
            return -errno.EACCES


    ###################################
    ### Routine: mknod              ###
    ###                             ###
    ### Used by: Client.mknod       ###
    ###################################

    def mknod( self, path, mode, device ):
    
        try:
            op_result = os.mknod( self.root + path, mode, device )
        except:
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: utime              ###
    ###                             ###
    ### Used by: Client.utime       ###
    ###################################

    def utime( self, path, times ):
    
        try:
            op_result = os.utime( self.root + path, times )
        except:
            op_result = -errno.EACCES
            
        return op_result


    ###################################
    ### Routine: access             ###
    ###                             ###
    ### Used by: Client.access      ###
    ###################################

    # TODO: This looks incorrect, look into it
    def access( self, path, mode ):
    
        try:
            if not os.access( self.root + path, mode ):
                raise
        except:
            return -errno.EACCES


#################
###   MAIN    ###
#################
    
# Main program.
def main():

    # Set default options.
    hostname = '0.0.0.0'
    port = 3636
    foreground = False
    secure = False
    mountpoint = "~/server"

    parser = argparse.ArgumentParser()

    parser.add_argument( "mountpoint", help="A directory or mount point must be specified" )
    parser.add_argument( '-v', '--version', action='store_true', help="Prints version information for the chunk server" )
    #parser.add_argument( '-a', '--address', dest=hostname, help="Used to specify the ip address of the chunk server" )
    #parser.add_argument( '-p', '--port', dest=port, help="User to specify the port the chunk server listens on" )
    #parser.add_argument( '-s', '--secure', action='store_true', dest=foreground, help="Run in secure mode using x509 certificate authentication" )
    #parser.add_argument( '-f', '--foreground', action='store_true', dest=secure, help="Run the chunk server in the foreground" )

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
        if not foreground:
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
        
        if secure:
            daemon = Pyro.core.Daemon(prtcol='PYROSSL', host=hostname, port=port)
            daemon.setNewConnectionValidator(MushroomCertValidator())
        else:
            #daemon = Pyro.core.Daemon(prtcol='PYRO', host=hostname, port=port)
            import netifaces
            ip = netifaces.ifaddresses('eth0')[2][0]['addr']
            daemon = Pyro.core.Daemon('PYRO',ip)
            
        # Use persistent connection (we don't want to use a Pyro
        # nameserver, to keep the things simple).
        #uri = daemon.connectPersistent(master_server, 'MushroomFS')
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


            
