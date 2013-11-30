import logging
import os, sys, threading, time, time_uuid
from errno import *
from stat import *
from operator import itemgetter

logging.basicConfig( filename='mushroom_client.log', level=logging.DEBUG )


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
        logging.debug( 'in init for Client' )
        # Initialize Fuse object
        Fuse.__init__( self, *args, **kw )
        
        # Initialize Pyro Client
        Pyro.core.initClient(0)
        
        # Instance Variables
        self.connected_master = False   # Connection status for Master
        self.connected_chunk = False    # Connection status for Chunk
        self.timestamp = 0              # Timestamp for data syncing
        self.last_offset = 0 
        self.chunk_counter = 0 
    ###############################################
    ### Subroutine: Connect Master Server       ###
    ###                                         ###
    ### Used by: reconnect_master_server, MAIN  ###
    ###############################################
    def connect_master_server( self ):
        logging.debug( 'in connect master server' )
        
        if self.connected_master:
            return
        
        # Try getting the Pyro proxy object for the Master Server    
        try:
        
            # Set protocol to use for Pyro RPC (secured or not)
            protocol = "PYRONAME://192.168.1.22/MushroomFS"
                
            # Get the master server proxy object from Pyro RPC system
            self.master_server = Pyro.core.getProxyForURI( protocol)
            
            # Check that the returned Pyro proxy object works
            if self.master_server.getattr('/'):
                self.connected_master = True
                self.timestamp = time.time()
            else:
                raise
                
        except Exception, error:
            print str(error)
            

    #############################################################
    ### Subroutine: Connect Client Server                     ###
    ###                                                       ###
    ### Used by: reconnect_chunk_server, rename_chunks        ###
    ###          MushroomFile.read, MushroomFile.write_chunks ###
    #############################################################

    def connect_chunk_server( self, chunkserver_name):
        logging.debug( 'in connect_chunk_server' )

        #if self.connected_chunk:
            #logging.debug( 'Some how we are connected already' )
            #return

        logging.debug( 'not already connected to chunk server') 
        # Try getting the Pyro proxy object for the Master Server    
        try:
        
            # Set protocol to use for Pyro RPC (secured or not)
            protocol = "PYRONAME://192.168.1.22/" + chunkserver_name
            logging.debug( 'Chunk server protocol is')
            logging.debug( protocol )   
            # Get the master server proxy object from Pyro RPC system
            logging.debug('CONNECTING TO CHUNK SERVER')
            self.chunk_server = Pyro.core.getProxyForURI( protocol )
            logging.debug( 'Connected to chunk server' )
            # Check that the returned Pyro proxy object works
            if self.chunk_server.getattr('/'):
                self.connected_chunk = True
                self.timestamp = time.time()
            else:
                logging.debug( 'Got an exception in connect chunk' )
                raise
                
        except Exception, error:
            print str(error)
            

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
        logging.debug( 'in reconnect_master_server' )
        
        #reestablish new lock
        self.lock.acquire()
        #toggle the master connection status to false
        self.connected_master = False
        
        #Attempt to rebind to the master server
        self.master_server.rebindURI()
        
        #call method to connect to the master server again 
        self.connect_master_server()


    ###################################################
    ### Subroutine: Reconnect Chunk Server          ###
    ###                                             ###
    ### Used by:   rename_chunks, MushroomFile.read ###
    ###################################################
        
    def reconnect_chunk_server( self):
        logging.debug( 'in reconnect_chunk_server') 
        #release the previous lock
        
        #toggle the chunk connection status to false       
        self.connected_chunk = False

        #Attempt to rebind to the master server
        self.chunk_server.rebindURI( tries=3, wait=3 )

        
    ##########################
    ### Routine: statfs    ###
    ##########################

    def statfs( self ):
        logging.debug( 'in statsfs' )
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                stats = self.master_server.statfs()
                
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
        logging.debug( 'in getattr' )
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                logging.debug( 'In getattr path below')
                logging.debug( path ) 
                #call to the master server to perform operation
                attr = self.master_server.getattr( path  )
                logging.debug( 'Attribyes from getattr on master below' )
                logging.debug( attr ) 
                
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
    
        logging.debug( 'in readlink' )
        
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                link = self.master_server.readlink( path )
                
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
        logging.debug( 'in readdir' ) 
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform iterative operations
                direntries = ['.', '..']
                direntries.extend( self.master_server.readdir( path[1:] ) )
                for item in direntries:
                    #as per FUSE docs use yield 
                    yield fuse.Direntry( item )
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                self.reconnect_master_server()
                
  
    ##########################
    ### Routine: truncate  ###
    ##########################

    def truncate( self, path, length ):
        logging.debug( 'in truncate' )
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                truncated_file = self.master_server.truncate( path, length )
                
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
        logging.debug( 'in rename' )    
        successful = False
        actual_renames = {}
        
        while not successful:
        
            try:
            
                if( self.timestamp != client.timestamp ):
                    self._reinitialize()
                else:
                
                    if client.master_server.exists( source_path ):
                        if client.master_server.exits( target_path ):
                            op_result = -errno.EACCES
                        else:
                            chunk_ids_list = client.master_server.get_chunk_ids( source_path )
                            actual_renames = self.rename_chunks( chunk_ids_list, target_path )
                            self.master_server.rename( source_path, target_path )
                        
                    else:
                        op_result = -errno.EACCES
                        
                    successful = True
            except:
                client.reconnect_master_server()
                self._reinitialize()
                
        client.master_server.register_chunks( actual_renames )
        return op_result

            
    ##################################
    ### Subroutine: rename chunks  ###
    ###                            ###
    ### Used by: rename            ###
    ##################################
        
    def rename_chunks( chunk_ids_list, target_path ):
        logging.debug( 'in rename_chunks' )    
    
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

                    while( chunk_ids_list ):
                        chunk_id = chunk_ids_list[0]
                        logging.debug('before modolus')
                        chunk_server_index = ( chunk_server_index + 1 ) % len( chunk_server_list )
                        logging.debug('after modolus')
                        chunk_location = chunk_server_list[ chunk_server_index ]
                        client.connect_chunk_server( chunk_location )
                        uuid = chunk_id[0]
                        new_chunk_id = ( uuid, target_path )
                        client.chunk_server.rename( chunk_id, new_chunk_id ) 
                    successful_chunk = True
                except:
                    chunk_server_index = (chunk_server_index + 1) % len( chunk_server_list )
                    self.reconnect_chunk_server( )

            return actual_renames
    
    ##########################
    ### Routine: mkdir     ###
    ##########################
            
    def mkdir( self, path, mode ):
        logging.debug( 'in mkdir' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                dir = self.master_server.mkdir( path, mode )
                
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
        logging.debug( 'in rmdir' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                remove_result = self.master_server.rmdir( path )
                
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
        logging.debug( 'in symlink' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                symlink_result = self.master_server.symlink( source_path, target_path )
                
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
        logging.debug( 'in link' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                link_result = self.master_server.link( source_path, target_path )
                
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
        logging.debug( 'in unlink' ) 
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                unlink_result = self.master_server.unlink( path )
                
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
        logging.debug( 'in chmod' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                chmod_result = self.master_server.chmod( path, mode )
                
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
        logging.debug( 'in chown' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                chown_result = self.master_server.chown( path, user, group )
                
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
        logging.debug( 'in mknod' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                nod = self.master_server.mknod( path, mode, dev )
                
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
        logging.debug( 'in utime' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                utime_result = self.master_server.utime( path, times )
                
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
        logging.debug( 'in access' )    
    
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform operation
                access_result = self.master_server.access( path, mode)
                
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
            logging.debug( 'in init for file' )    
        
            #set status for class init as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: to contact the master & Try2: to open file
                try:
                    
                    #call to the master server to open file on its end
                    ret = client.master_server.open( path, flags, mode )
                    
                    #Use the client's timestamp for this file's timestamp
                    self.timestamp =  client.timestamp
                    
                    #Set param as this file's class instance var
                    self.path = path
                    self.flags = flags
                    self.file_descriptor = ret
                    if mode:
                        self.mode = mode[0]
                    
                    #change operation status to successul & exit loop
                    successful = True;
                
                #In case of file failure
                except Exception as error:
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
            logging.debug( 'in reinit' )    
        
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

        def get_num_chunks(self, size, chunk_size ):
            logging.debug( 'in get_num_chunks' )    
        
            # Nmbr of Chunks must be an int so must round any fraction up to next int
            return ( size + chunk_size - 1 ) // chunk_size
                
                
        """
        FILE SYSTEM ROUTINES
        """

        #############################
        ### Routine: MF.release   ###
        #############################

        def release( self, flags ):
            logging.debug( 'in release' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master
                try:
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #and toggle as successful without doing anything else
                        successful = True
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to release the file
                        client.master_server.release( self.file_descriptor, flags )
                        client.last_offset = 0
                        client.chunk_counter = 0
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    client.reconnect_master_server()


        ##############################
        ### Routine: MF.ftruncate  ###
        ##############################
            
        def ftruncate( self, len ):
            logging.debug( 'in ftruncate' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        ftrunc_result = client.master_server.ftrucate( self.file_descriptor, len )
                        
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
            logging.debug( 'in read' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: connect master, Try2: open file
                try:
                
                    #Verify that the file's timestamp is valid
                    #if( self.timestamp != client.timestamp ):
                    if( False ):    
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        logging.debug( 'In else of read' )
                        #init a list that holds all the chunked data segments
                        data_chunks_list = []
                        
                        #contact master & get all this file's chunk's IDs
                        chunk_ids_list = client.master_server.get_chunk_ids( self.file_descriptor, self.path )
                        logging.debug( 'Got chunk ids' )
                        logging.debug( chunk_ids_list )

                        #sort the chunk IDS such that they are in order
                        sorted_chunk_ids_list = sorted( chunk_ids_list, key=itemgetter( 0 ) )
                        logging.debug( 'Sorted Chunks' )
                        logging.debug( sorted_chunk_ids_list ) 

                        chunk_size = client.master_server.get_chunk_size()
                        logging.debug( 'Got chunk size' )
                        logging.debug( chunk_size )
                        logging.debug( 'last offset' )
                        logging.debug( client.last_offset )
                        #call to subroutine, returns the # of chunks to split data into
                        previous_num_chunks = self.get_num_chunks( client.last_offset, chunk_size )
                        logging.debug( 'previous_num_chunks' )
                        logging.debug( previous_num_chunks )
                        num_chunks = self.get_num_chunks( size, chunk_size )
                        logging.debug( 'Number Chunks' )
                        logging.debug( num_chunks )

                        client.last_offset = client.last_offset + size

                        #for every chunk ID for this file
                        for chunk_id in sorted_chunk_ids_list[previous_num_chunks:num_chunks]:
                            logging.debug( 'CHUNK ID FOR LOOP' )
                            logging.debug( chunk_id )
                            logging.debug( 'CHUNK COUNTER' )
                            logging.debug( client.chunk_counter )
                            client.chunk_counter = client.chunk_counter + 1
                            logging.debug( 'In sorted chunks for loop' )
                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            uuid = chunk_id[0]
                            file_path = chunk_id[1]
                            chunk_name = str( uuid ) + "--" + file_path
                            logging.debug( 'Chunk name is')
                            logging.debug( chunk_name ) 
                            logging.debug('contacting master for chunk locations')
                            logging.debug('the key for the call is:')
                            logging.debug( chunk_id ) 
                            logging.debug('the master is connected:')
                            logging.debug(client.connected_master)
                            logging.debug('calling master to connect')
                            #client.connect_master_server()
                            #contact master & using ID get the location of this chunk
                            chunk_locations_list = client.master_server.get_chunkloc( uuid )
                            logging.debug('chunk location is:')
                            logging.debug(chunk_locations_list)
                            successful_chunk_read = False
                            chunk_location_index = 0

                            while not successful_chunk_read:
                                logging.debug('inside the chunk reading while-loop')
                            
                                #master returns a list of servers with that chunk, pick the 1st
                                chunk_location = chunk_locations_list[0]
                                logging.debug( 'in read, using chunk location: ' )
                                logging.debug( chunk_location )
                                #Try3: connect to chunk servers
                                try:
                                    #Connect to proper chunk server for this chunk
                                    client.connect_chunk_server( chunk_location )
                                    logging.debug( 'in read connected to chunk server' ) 
                                    #Read the chunk data from chunk server
                                    data_chunk = client.chunk_server.read( chunk_name )
                                    logging.debug( 'in read, read chunk from server' )
                                    logging.debug( 'chunk data' )
                                    logging.debug( data_chunk ) 
                                    #add this chunk's data to the list
                                    data_chunks_list.append( data_chunk )
                                    logging.debug( 'in read, appended to chunks list' )
                                    logging.debug( data_chunks_list )
                                    successful_chunk_read = True
                            
                                #In case of chunk server connection failure, reconnect
                                except:
                                    chunk_location_index = ( chunk_location_index + 1 ) % len( chunk_location )
                                    client.reconnect_chunk_server()
                        
                        #convert file data into binary data
                        data = b"".join( data_chunks_list )
                        logging.debug( 'Joined dat in read' )
                        logging.debug( data )
                
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

        def write( self, buf, offset ):
            #initialize operation as not successful
            successful = False
            actual_writes = {}
            #continue until we are successful
            while not successful:
                logging.debug( 'In while loop of write' )
                #Try for connection to master
                try:
                    logging.debug( 'In try block, of write' )
                    
                    #Verify that the file's timestamp is valid
                    #if( self.timestamp != client.timestamp ):
                    if( False ):    
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        logging.debug( 'Inside else of write' )
                        #if the file already exists on master then overwrite it 
                        #if client.master_server.exists():
                        #    client.ftruncate( self.path  )
                         
                        #contact master and get the chunk size 
                        chunk_size = client.master_server.get_chunk_size()
                        logging.debug( 'Got chunk size' )
                        logging.debug( chunk_size )
                        #call to subroutine, returns the # of chunks to split data into
                        num_chunks = self.get_num_chunks( len( buf), chunk_size )
                        logging.debug( 'Number Chunks' )
                        logging.debug( num_chunks )
                        #contact master to generate a unique id for each chunk
                        chunk_ids_list = client.master_server.generate_chunk_ids( self.file_descriptor, self.path, num_chunks, len( buf ) )
                        logging.debug( 'Got chunk ids' )
                        logging.debug( chunk_ids_list )
                        #call to subroutine to write each chunk to the appropriate chunk server
                        actual_writes = self.write_chunks( chunk_ids_list, buf, chunk_size )
                        logging.debug( 'Wrote chunks' )
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of master connection failure, reconnect & reset timestamps
                except:
                    logging.debug( 'Got exception while in write()' )
                    client.reconnect_master_server()
                    self._reinitialize()
            
            #After successful write, confirm with master which chunkserver's belong with each ID
            successful_confirm = False

            while not successful_confirm:
                try:
                    logging.debug( 'trying to write actual write' )
                    logging.debug( actual_writes )
                    client.master_server.register_chunks( actual_writes )
                    logging.debug( 'Successfully sent actual writes' )
                    successful_confirm = True
                except:
                    client.reconnect_master_server()
            #return the length of buffer to FUSE 
            return len( buf )

            
        ######################################
        ### Subroutine: MF.write_chunks    ###
        ###                                ###
        ### Used by:   MF.write            ###
        ######################################
            
        def write_chunks(self, chunk_ids_list, buf, chunk_size ):
            logging.debug( 'in write_chunks' )    
        
            #writing status with master
            successful_master = False
            
            #writing status with chunk servers
            successful_chunk = False
            #splice original data into chunks where size of chunks defined by master
            data_chunks_list = [ buf[index:index + chunk_size] for index in range(0, len( buf ), chunk_size ) ]
            logging.debug( 'Data chunks' )
            logging.debug( data_chunks_list )
            logging.debug( 'Got chunks split up' ) 
            #init dict holding chunks ids & servers that have already successfully written (in case of fail)
            actual_writes = {}
            chunk_server_list = [] 
            #continue until we are successful & done with master
             
            while not successful_master:
                
                #Try for connection to master
                try:
                    logging.debug( 'Try to connect to master for chunk servers' )
                    #Get a full list of active chunk servers from the master
                    chunk_server_list = client.master_server.get_chunk_servers()
                    logging.debug( 'Chunk server list' )
                    logging.debug( chunk_server_list )
                    #finished with master, move to next step
                    successful_master = True
                
                #In case of master connection failure then try to reconnect
                except Exception:
                    logging.debug( 'Exception at get_chunk_servers')
                    client.reconnect_master_server()
            #Next, as long as we have chunk servers then try to write chunks to them
            
            if chunk_server_list:
            
                #continue until we are successful & done with writing all chunks
                while not successful_chunk:
                
                    #Try to write all chunks to the chunk servers using round robin (from list)
                    try:
                        #for each chunk of data
                        logging.debug( 'Trying to iterate over chunks' )
                        for index in range( 0, len( data_chunks_list ) ):
                            logging.debug( 'Index of for look in write chunks: ' )
                            logging.debug( index )
                            #get the key that associates to that chunk
                            chunk_id = chunk_ids_list[ index ]
                            logging.debug( 'Got chunk id' )
                            logging.debug( chunk_id )
                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            uuid = str( chunk_id[0] )
                            file_path = chunk_id[1]
                            chunk_name = uuid + "--" + file_path
                            logging.debug( 'Got chunk name' )
                            logging.debug( chunk_name )
                            #Change which chunk server will get the next chunk (cycles through chunk severs)
                            logging.debug('CHUNK SERVER LIST CONTENTS')
                            logging.debug(chunk_server_list)
                            chunk_server_index = index % len( chunk_server_list )
                            logging.debug( 'Got chunk server index' )
                            logging.debug( chunk_server_index )
                            #from list get location of where chunk should go (i.e. which chunkserver)
                            chunk_location = chunk_server_list[ chunk_server_index ]
                            logging.debug( 'Got chunk location' )
                            logging.debug( chunk_location )
                            logging.debug( 'Trying to connect to chunk server at location' )
                            logging.debug( chunk_location ) 
                            #connect to that chunk server
                            client.connect_chunk_server( chunk_location )
                            logging.debug( 'Connected to chunk server' )
                            #write that chunk data to the chunk server
                            client.chunk_server.write( data_chunks_list[ 0 ], chunk_name )
                            logging.debug( 'Completed writes to chunk server' )
                            logging.debug( 'About to delete data_chunk at index: ' )
                            logging.debug( index )
                            logging.debug( 'Length of data_chunks_list is: ' )
                            logging.debug( len( data_chunks_list ) )
                            #delete that chunk from list (in case of failure: only failed chunks retry)
                            del data_chunks_list[ 0 ]
                            
                            #add the chunk id and its chunk server to the dictionary of actual writes
                            logging.debug( 'CHUNK ID AND LOCATION IN write_chunks/actual_writes' )
                            logging.debug( chunk_id )
                            logging.debug( chunk_location )
                            actual_writes[ chunk_id ] = chunk_location
                            logging.debug( 'Actual Writes dict' )
                            logging.debug( actual_writes ) 
                         #finished with writing  
                        successful_chunk = True
                    
                    #In case of failure due to inactive server, then remove that server from list & try again
                    
                    except Exception, error:
                        logging.debug( 'Got exception in write chunks try actually writing chunks' )
                        logging.debug( error )
                        logging.debug( 'Removeing chunk server from list' )
                        logging.debug( chunk_server_list[ chunk_server_index ] )
                        del chunk_server_list[ chunk_server_index ]
             
            #return the dict back to write method
            return actual_writes
                
                
        ###############################
        ### Routine: MF.fgetattr    ###
        ###############################

        def fgetattr( self ):
            logging.debug( 'in fgetattr' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                    
                    #Verify that the file's timestamp is valid
                    #if( self.timestamp != client.timestamp ):
                    if( False ):
                        logging.debug( 'in fgetattr, time stamps dont match' )
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        logging.debug( 'in fgetattr, calling master fgetattr' )
                        attr = client.master_server.fgetattr( self.file_descriptor, self.path )
                        logging.debug( 'in fgetattr, got master fstat' )
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    logging.debug( 'in fgetattr, got exception' )
                    client.reconnect_master_server()
                    self._reinitialize()
                
            #return the master's resultant to FUSE        
            return attr
                
                
        ###############################
        ### Routine: MF.flush       ###
        ###############################

        def flush( self ):
            logging.debug( 'in flush' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                
                    #Verify that the file's timestamp is valid
                    #if( self.timestamp != client.timestamp ):
                    if( False ):    
                         #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        #Tell the master (via client) to perform file operation
                        flush_result = client.master_server.flush( self.file_descriptor )
                        
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
            logging.debug( 'in fsync' )    
        
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try to contact the master (via client)
                try:
                    
                    #Verify that the file's timestamp is valid
                    #if( self.timestamp != client.timestamp ):
                    if( False ):    
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        
                        #Tell the master (via client) to perform file operation
                        fsync_result = client.master_server.fsync( self.file_descriptor, isfsyncfile )
                        
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


#################
###   MAIN    ###
#################

# Main program.
def main():
    
    #init a global for MushroomClient so its accessible by MushroomFile class 
    global client

    # Initialize the PyGFS client object.
    client = MushroomClient(version=fuse.__version__, dash_s_do='setsingle')
    logging.debug( 'In client main, client created' )
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
            while client.connected_master == False:
                client.connect_master_server()
                time.sleep(1)
    try:
        # call the MushroomClient's main method to Mount the filesystem.
        logging.debug( 'ABout to start main')
        client.main()
    except Exception, error:
        # Unmount the filesystem.
        if client.parser.fuse_args.mountpoint:
            print >> sys.stderr,"umounting " + str( client.parser.fuse_args.mountpoint )

if __name__ == '__main__':
        main()
