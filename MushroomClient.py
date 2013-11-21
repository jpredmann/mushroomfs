
import os, sys, threading, time, time_uuid
from errno import *
from stat import *
from operator import itemgetter

# Try importing Fuse, generate an error message and exit if it cannot import Fuse
# Using Fuse-Python 0.2
try:
    import fuse
    from fuse import Fuse, FuseArgs
except ImportError:
    print >> sys.stderr, "Fuse does not appear to be properly installed"
    sys.exit( 1 )
    
# Try importing Pyro, generate an error message and exit if it cannot import Pyro
# Using Pyro version 3.16
try:
    import Pyro.core
    import Pyro.protocol
except ImportError:
    print >> sys.stderr, "Pyro does not appear to be properly installed"
    sys.exit( 1 )
    
    
# Initialize the fuse api environment
# Set fuse version number
fuse.fuse_python_api = ( 0, 2 )
# ??
fuse.feature_assert( 'stateful_files' )


# Generate an error if the fuse api cannot interact with the Fuse kernel module
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."


#######################
### MUSHROOM CLIENT ###
#######################

# Mushroom client class, inherits from class Fuse
class MushroomClient(Fuse):
    """
    This class will process all system calls received by the Fuse module
    """

    
    ############################
    ### Class Initialization ###
    ############################
    
    def __init__( self, *args, **kw ):
    
        # Initialize Fuse object
        Fuse.__init__( self, *args, **kw )
        
        # Initialize Pyro Client
        Pyro.core.initClient(0)
        
        # Instance Variables
        self.lock = threading.Lock()    # Used for Locking & Blocking
        self.host_master = '127.0.0.1'  # IP for Master
        self.port_master = 3636         # Port for Master
        self.authentication_on = False  # For SSL
        self.connected_master = False   # Connection status for Master
        self.connected_chunk = False    # Connection status for Chunk
        self.timestamp = 0              # Timestamp for data syncing
        
        
    
    ###############################################
    ### Subroutine: Connect Master Server       ###
    ###                                         ###
    ### Used by: reconnect_master_server, MAIN  ###
    ###############################################
    #TODO: Make parametric to connect to either chunk or master servers
    def connect_master_server( self ):
        
        # Get a lock to talk to the server, if a client thread is already talking to
        # the server release the lock
        self.lock.acquire()
        
        #TODO: This code and the lock acquire are ugly, needs to be replaced
        if self.connected_master:
            self.lock.release()
            return
        
        # Try getting the Pyro proxy object for the Master Server    
        try:
        
            # Set protocol to use for Pyro RPC (secured or not)
            if self.authentication_on:
                protocol = "PYROLOCSSL://"
            else:
                protocol = "PYROLOC://"
                
            # Get the master server proxy object from Pyro RPC system
            self.master_server = Pyro.core.getProxyForURI( protocol + self.host_master + ":" + str(self.port_master) + "/MushroomFS" )
            
            # Check that the returned Pyro proxy object works
            if self.master_server.getattr('/'):
                self.connected_master = True
                self.timestamp = time.time()
            #TODO finish implementing this raise 
            else:
                raise
                
        except Exception, error:
            print str(error)
            
        # Release the lock so that other threads can acquire it
        self.lock.release()
    

    #############################################################
    ### Subroutine: Connect Client Server                     ###
    ###                                                       ###
    ### Used by: reconnect_chunk_server, rename_chunks        ###
    ###          MushroomFile.read, MushroomFile.write_chunks ###
    #############################################################

    #TODO: Make parametric to connect to either chunk or master servers
    def connect_chunk_server( self, ip, port ):
        
        # Get a lock to talk to the server, if a client thread is already talking to
        # the server release the lock
        self.lock.acquire()
        
        #TODO: This code and the lock acquire are ugly, needs to be replaced
        if self.connected_chunk:
            self.lock.release()
            return
        
        # Try getting the Pyro proxy object for the Master Server    
        try:
        
            # Set protocol to use for Pyro RPC (secured or not)
            if self.authentication_on:
                protocol = "PYROLOCSSL://"
            else:
                protocol = "PYROLOC://"
                
            # Get the master server proxy object from Pyro RPC system
            self.chunk_server = Pyro.core.getProxyForURI( protocol + str( ip ) + ":" + str(port) + "/PyGFS" )
            
            # Check that the returned Pyro proxy object works
            if self.chunk_server.getattr('/'):
                self.connected_chunk = True
                self.timestamp = time.time()
            #TODO finish implementing this raise
            else:
                raise
                
        except Exception, error:
            print str(error)
            
        # Release the lock so that other threads can acquire it
        self.lock.release()

            
            
    ##################################################################
    ### Subroutine: Reconnect Master Server                        ###
    ###                                                            ###
    ### Used by: statfs, getattr, readlink, readdir, truncate,     ###
    ###          rmdir,rename, rename_chunks, mkdir, symlink,      ###
    ###          link, unlink,chmod, chown, mknod, utime, access,  ###
    ###          MushroomFile.__init__, MushroomFile.release,      ###
    ###          MushroomFile.ftruncate, MushroomFile.read,        ###
    ###          MushroomFile.write, MushroomFile.write_chunks,    ###
    ###          MushroomFile.fgetattr, MushroomFile.flush,        ###
    ###          MushroomFile.fsync                                ###
    ##################################################################
                
    def reconnect_master_server( self ):
        
        #release the previous lock
        try:
            self.lock.release()
        except:
            pass
        
        #reestablish new lock
        self.lock.acquire()

        #toggle the master connection status to false
        self.connected_master = False
        
        #relese the lock
        self.lock.release()
        
        #Attempt to rebind to the master server
        self.master_server.rebindURI()
        
        #call method to connect to the master server again 
        self.connect_master_server()


    ###################################################
    ### Subroutine: Reconnect Master Server         ###
    ###                                             ###
    ### Used by:   rename_chunks, MushroomFile.read ###
    ###################################################
        
    def reconnect_chunk_server( self ):
    
        #release the previous lock
        try:
            self.lock.release()
        except:
            pass
        
        #reestablish new lock
        self.lock.acquire()

        #toggle the chunk connection status to false       
        self.connected_chunk = False

        #release the lock
        self.lock.release()

        #Attempt to rebind to the master server
        self.chunk_server.rebindURI( tries=10, wait=3 )

        
    ##########################
    ### Routine: statfs    ###
    ##########################

    def statfs( self ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                stats = self.master_server.statfs()
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE 
        return stats
        

    ##########################
    ### Routine: getattr   ###
    ##########################

    def getattr( self, path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                attr = self.master_server.getattr( path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return attr


    ##########################
    ### Routine: readlink  ###
    ##########################
        
    def readlink( self, path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                link = self.master_server.readlink( path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return link

        
    ##########################
    ### Routine: readdir   ###
    ##########################
        
    def readdir( self, path, offset ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                # TODO: Add calls to chunk servers
                
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform iterative operations
                direntries = ['.', '..']
                direntries.extend( self.master_server.readdir( path[1:] ) )
                for item in direntries:
                    #as per FUSE docs use yield 
                    yield fuse.Direntry( item )
                
                #release the lock
                self.lock.release()
            
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
                
  
    ##########################
    ### Routine: truncate  ###
    ##########################

    def truncate( self, path, length ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                # TODO: Add calls to chunk servers
                
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                truncated_file = self.master_server.truncate( path, length )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()

        #return the master's resultant to FUSE        
        return truncated_file
            
            
    ##########################
    ### Routine: rename    ###
    ##########################
        
    def rename( self, source_path, target_path  ):
    
        successful = False
        rename_result = {}
        
        while not successful:
        
            try:
            
                client.lock.acquire()
                if( self.timestamp != client.timestamp ):
                    client.lock.release()
                    self._reinitialize()
                else:
                
                    if client.master_server.exists( source_path ):
                        if client.master_server.exits( target_path ):
                            op_result = -errno.EACCES
                        else:
                            chunk_ids = client.master_server.get_chunk_ids( source_path )
                            self.rename_chunks( chunk_ids, target_path )
                            self.master_server.rename( source_path, target_path )
                        
                    else:
                        op_result = -errno.EACCES
                        
                    client.lock.release()
                    successful = True
            except:
                client.reconnect_master_server()
                self._reinitialize()
                
        client.master_server.confirm_write( write_results )
        return op_result

            
    ##################################
    ### Subroutine: rename chunks  ###
    ###                            ###
    ### Used by: rename            ###
    ##################################
        
    def rename_chunks( chunk_ids, target_path ):
    
        successful_master = False
        successful_chunk = False
        actual_renames = {}
        
        while not successful_master:
        
            try:
                chunk_server_list = client.master_server.get_chunk_servers()
                successful_master = True
            except:
                client.reconnect_master_server()
        
        if chunk_server_list:
       
            chunk_server_index = 0
             
            while not successful_chunk:
            
                try:
                    """
                    for index in range( 0, len( chunk_ids ) ):
                        chunk_id = chunk_ids[ index ]
                        chunk_server_index = index % len( chunk_server_list )
                        chunk_location = chunk_server_list[ chunk_server_index ]
                        ip = chunk_location[0]
                        port = chunk_location[1]
                        client.connect_chunk_server( ip, port )
                        new_chunk_id = ( chunk_id[0], target_path )
                        client.chunk_server.rename( chunk_id, new_chunk_id )
                    """

                    while( chunk_ids ):
                        chunk_id = chunk_ids[0]
                        chunk_server_index = ( chunk_server_index + 1 ) % len( chunk_server_list )
                        chunk_location = chunk_server_list[ chunk_server_index ]
                        ip = chunk_location[0]
                        port = chunk_location[1]
                        client.connect_chunk_server( ip, port )
                        new_chunk_id = ( chunk_id[0], target_path )
                        client.chunk_server.rename( chunk_id, new_chunk_id ) 
                    successful_chunk = True
                except:
                    chunk_server_index = (chunk_server_index + 1) % len( chunk_server_list )
                    self.reconnect_chunk_server( )

    
    ##########################
    ### Routine: mkdir     ###
    ##########################
            
    def mkdir( self, path, mode ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                dir = self.master_server.mkdir( path, mode )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
            
        #return the master's resultant to FUSE        
        return dir


    ##########################
    ### Routine: rmdir     ###
    ##########################

    def rmdir( self, path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #TODO: Add calls to chunk servers
                
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                remove_result = self.master_server.rmdir( path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
         
        #return the master's resultant to FUSE 
        return remove_result
    

    ##########################
    ### Routine: symlink   ###
    ##########################
        
    def symlink( self, source_path, target_path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                symlink_result = self.master_server.symlink( source_path, target_path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE 
        return symlink_result

        
    ##########################
    ### Routine: link      ###
    ##########################
        
    def link( self, source_path, target_path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                link_result = self.master_server.link( source_path, target_path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return link_result
        

    ##########################
    ### Routine: unlink    ###
    ##########################
        
    def unlink( self, path ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                unlink_result = self.master_server.unlink( path )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
            
        #return the master's resultant to FUSE
        return unlink_result
        

    ##########################
    ### Routine: chmod     ###
    ##########################
        
    def chmod( self, path, mode ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                chmod_result = self.master_server.chmod( path, mode )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return chmod_result
        
        
    ##########################
    ### Routine: chown     ###
    ##########################
        
    def chown( self, path, user, group ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                chown_result = self.master_server.chown( path, user, group )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return chown_result
        

    ##########################
    ### Routine: mknod     ###
    ##########################
        
    def mknod( self, path, mode, dev ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                nod = self.master_server.mknod( path, mode, dev )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        #return the master's resultant to FUSE       
        return nod
        
        
    ##########################
    ### Routine: utime     ###
    ##########################
        
    def utime( self, path, times ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                utime_result = self.master_server.utime( path, times )
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return utime_result
        
        
    ##########################
    ### Routine: access    ###
    ##########################

    def access( self, path, mode ):
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                # TODO: Add calls to chunk servers
                
                #block all other processes on client
                self.lock.acquire()
                
                #call to the master server to perform operation
                access_result = self.master_server.access( path, mode)
                
                #release the lock
                self.lock.release()
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()

        #return the master's resultant to FUSE        
        return access_result

        
    #######################
    ### MUSHROOM FILE   ###
    #######################
        
    class MushroomFile():
        
        ############################
        ### Class Initialization ###
        ############################
    
        def __init__( self, path, flags, *mode ):
        
            #set status for class init as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: to contact the master & Try2: to open file
                try:
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #call to the master server to open file on its end
                    self.file_desctriptor = client.master_server.open( path, flags, mode )
                    
                    #Use the client's timestamp for this file's timestamp
                    self.timestamp =  client.timestamp
                    
                    #release the lock
                    client.lock.release()
                
                    #Set param as this file's class instance var
                    self.path = path
                    self.flags = flags
                    if mode:
                        self.mode = mode[0]
                    
                    #change operation status to successul & exit loop
                    successful = True;
                
                #In case of file failure
                except FileNotFoundException as error:
                    raise error
            
                #In case of connection failure
                except:
                    client.reconnect_master_server()
            
            #Return error if file descriptot doesn't index correctly (i.e. positive int)
            if self.file_descriptor < 0:
                raise OSError( errno.EACCES, "Premission denied: " + self.path )
        
        
        ###################################################
        ### Subroutine: MF._reinitialize                ###
        ###                                             ###
        ### Used by:   MF.ftruncate, MF.read, MF.write, ###
        ###            MF.fgetattr, MF.flush, MF.fsync  ###
        ###################################################
                
        def _reinitialize( self ):
        
            #check to see if this file had a mode param
            if hasattr( self, 'mode' ):
                
                #if so, call the file class constructor with mode included
                self.__init__( self.path, self.flags, self.mode )
            
            else:
                #otherwise, call file class constructor without a mode  
                self.__init__( self.path, self.flags )


        ######################################
        ### Subroutine: MF.get_num_chunks  ###
        ###                                ###
        ### Used by:   MF.write            ###
        ######################################

        def get_num_chunks( size, chunk_size ):
        
            # Nmbr of Chunks must be an int so must round any fraction up to next int
            return ( size + chunk_size - 1 ) // chunk_size
                
                
        """
        FILE SYSTEM ROUTINES
        """

        #############################
        ### Routine: MF.release   ###
        #############################

        def release( self, flags ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master
                try:
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #and toggle as successful without doing anything else
                        successful = True
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to release the file
                        client.master_server.release( self.file_descriptor, flags )
                        
                        #then release the lock
                        client.lock.release()
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()


        ##############################
        ### Routine: MF.ftruncate  ###
        ##############################
            
        def ftruncate( self, len ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        ftrunc_result = client.master_server.ftrucate( self.file_descriptor, len )
                        
                        #then release the lock
                        client.lock.release()
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
               
            #return the master's resultant to FUSE 
            return ftrunc_result


        ##############################
        ### Routine: MF.read       ###
        ##############################

        def read( self, size, offset ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: connect master, Try2: open file
                try:
                
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #init a list that holds all the chunked data segments
                        data_chunks = []
                        
                        #contact master & get all this file's chunk's IDs
                        chunk_ids = client.master_server.get_chunk_ids( self.path )
                        
                        #sort the chunk IDS such that they are in order
                        sorted_chunk_ids = sorted( chunk_ids, key=itemgetter( 0 ) )
                        
                        #for every chunk ID for this file
                        for id in sorted_chunk_ids:
                        
                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            chunk_name = str( id[0] ) + id[1]
                            
                            #contact master & using ID get the location of this chunk
                            chunk_location = client.master_server.get_chunkloc( chunk_name )

                            successful_chunk_read = False
                            chunk_location_index = 0

                            while not successful_chunk_read:
                            
                                #master returns a list of servers with that chunk, pick the 1st
                                location = chunk_location[0]
                            
                                #chunk server's location is tuple: (ip address, port)
                                ip = location[0]
                                port = location[1]
                            
                                #Try3: connect to chunk servers
                                try:
                                    #Connect to proper chunk server for this chunk
                                    client.connect_chunk_server( ip, port )
                                
                                    #Read the chunk data from chunk server
                                    chunk = client.chunk_server.read( chunk_name )
                                
                                    #add this chunk's data to the list
                                    data_chunks.append( chunk )
                                    successful_chunk_read = True
                            
                                #In case of chunk server connection failure, reconnect
                                except:
                                    chunk_location_index = ( chunk_location_index + 1 ) % len( chunk_location )
                                    client.reconnect_chunk_server()
                        
                        #convert file data into binary data
                        data = b"".join( data_chunks )
                
                        #then release the lock
                        client.lock.release()
                
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of master connection failure, reconnect & reset timestamps
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
                
            #return the read binary data to FUSE
            return data
            

        ###############################
        ### Routine: MF.write       ###
        ###############################

        def write( self, buffer, offset ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try for connection to master
                try:
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                    
                        #if the file already exists on master then overwrite it 
                        # TODO: wat???? truncate amd delete???
                        if client.master_server.exists():
                            client.ftruncate( self.path  )
                        
                        #contact master and get the chunk size in bytes
                        chunk_size = client.master_server.get_chunk_size()
                
                        #call to subroutine, returns the # of chunks to split data into
                        num_chunks = client.get_num_chunks( len( buffer), chunk_size )
                
                        #contact master to generate a unique id for each chunk
                        chunk_ids = client.master_server.alloc( self.path, num_chunks )
                
                        #call to subroutine to write each chunk to the appropriate chunk server
                        write_result = self.write_chunks( chunk_ids, buffer, chunk_size )
                
                        #then release the lock
                        client.lock.release()
                
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of master connection failure, reconnect & reset timestamps
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
            
            #After successful write, confirm with master which chunkserver's belong with each ID
            #TODO write this method in master
            client.master_server.confirm_write( write_results )
                        
            #return the length of buffer to FUSE 
            return len( buffer )

            
        ######################################
        ### Subroutine: MF.write_chunks    ###
        ###                                ###
        ### Used by:   MF.write            ###
        ######################################
            
        def write_chunks( chunk_ids, buffer, chunk_size ):
        
            #writing status with master
            successful_master = False
            
            #writing status with chunk servers
            successful_chunk = False
            
            #splice original data into chunks where size of chunks defined by master
            chunks = [ buffer[index:index + chunk_size] for index in range(0, len( buffer ), chunk_size ) ]
            
            #init dict holding chunks ids & servers that have already successfully written (in case of fail)
            actual_writes = {}
            
            #continue until we are successful & done with master
            while not successful_master:
            
                #Try for connection to master
                try:
                    #Get a full list of active chunk servers from the master
                    chunk_server_list = client.master_server.get_chunk_servers()
                    
                    #finished with master, move to next step
                    successful_master = True
                
                #In case of master connection failure then try to reconnect
                except:
                    client.reconnect_master_server()
            
            #Next, as long as we have chunk servers then try to write chunks to them
            if chunk_server_list:
            
                #continue until we are successful & done with writing all chunks
                while not successful_chunk:
                
                    #Try to write all chunks to the chunk servers using round robin (from list)
                    try:
                        #for each chunk of data
                        for index in range( 0, len( chunks ) ):
                            
                            #get the key that associates to that chunk
                            chunk_id = chunk_ids[ index ]

                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            chunk_name = str( chunk_id[0] ) + chunk_id[1]

                            #Change which chunk server will get the next chunk (cycles through chunk severs)
                            chunk_server_index = index % len( chunk_server_list )
                            
                            #from list get location of where chunk should go (i.e. which chunkserver)
                            chunk_location = chunk_server_list[ chunk_server_index ]
                            
                            #Chunk location is a tuple: (ip address, port)
                            ip = chunk_location[0]
                            port = chunk_location[1]
                            
                            #connect to that chunk server
                            client.connect_chunk_server( ip, port )
                            
                            #write that chunk data to the chunk server
                            client.chunk_server.write( chunks[ index ], chunk_name )
                            
                            #delete that chunk from list (in case of failure: only failed chunks retry)
                            del chunks[ index ]
                            
                            #add the chunk id and its chunk server to the dictionary of actual writes
                            actual_writes[ chunk_id ] = chunk_location
                        
                         #finished with writing  
                        successful_chunk = True
                    
                    #In case of failure due to inactive server, then remove that server from list & try again
                    except:
                        client.reconnect_chunk_server()
                        del chunk_server_list[ chunk_server_index ]
            
            #return the dict back to write method
            return actual_writes
                
                
        ###############################
        ### Routine: MF.fgetattr    ###
        ###############################

        def fgetattr( self ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        attr = client.master_server.fgetattr( self.file_descriptor )
                        
                        #then release the lock
                        client.lock.release()
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
                
            #return the master's resultant to FUSE        
            return attr
                
                
        ###############################
        ### Routine: MF.flush       ###
        ###############################

        def flush( self ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                         #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        #Tell the master (via client) to perform file operation
                        flush_result = client.master_server.flush( self.file_descriptor )
                        
                        #then release the lock
                        client.lock.release()
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
                    
            #return the master's resultant to FUSE 
            return flush_result 

                
        ###############################
        ### Routine: MF.fsync       ###
        ###############################
            
        def fsync( self, isfsyncfile ):
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                    #block all other processes on client
                    client.lock.acquire()
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #if not, then relase the lock
                        client.lock.release()
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        fsync_result = client.master_server.fsync( self.file_descriptor, isfsyncfile )
                        
                        #then release the lock
                        client.lock.release()
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
             
            #return the master's resultant to FUSE 
            return fsync_result
                
                
    #############################
    ### Routine: main         ###
    ###                       ###
    ### Used by: MAIN         ###
    #############################
            
    def main( self, *file_args, **kw ):
        
        #Set MushroomFile class as "file_class" for fuse-python
        self.file_class = self.MushroomFile
        
        #Call & Return Fuse-python's main method
        return Fuse.main(self, *file_args, **kw )


##################################
### FILE NOT FOUND EXCEPTION   ###
##################################

class FileNotFoundException( Exception ):

    pass
            

#################
###   MAIN    ###
#################

# Main program.
def main():
    
    #init a global for MushroomClient so its accessible by MushroomFile class 
    global client

    # Initialize the PyGFS client object.
    client = MushroomClient(version=fuse.__version__, dash_s_do='setsingle')

    # Add custom options.
    client.parser.add_option(mountopt="host", metavar="HOSTNAME", default="127.0.0.1",
        help="The Mushroom server hostname [default: 127.0.0.1]")
    client.parser.add_option(mountopt="port", metavar="PORT", default="3636",
        help="The MushroomFS port on the server [default: 3636]")
    client.parser.add_option("-x", "--secure", action="store_true", dest="authentication_on",
        help="Run MushroomFS in secure mode, using x509 certificate authentication and encryption")

    # Parse user options.
    client.parse(values=client, errex=1)
    if (len(sys.argv) > 1):
        if not (client.fuse_args.getmod('showhelp') or client.fuse_args.getmod('showversion')):
            # Connect to the Mushroom Master Server.
            while client.connected_master == 0:
                client.connect_master_server()
                time.sleep(1)
    try:
        # call the MushroomClient's main method to Mount the filesystem.
        client.main()
    except Exception, error:
        # Unmount the filesystem.
        if client.parser.fuse_args.mountpoint:
            print >> sys.stderr,"umounting " + str( client.parser.fuse_args.mountpoint )

if __name__ == '__main__':
        main()
