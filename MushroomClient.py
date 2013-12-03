import logging
import base64
import os, sys, threading, time, time_uuid
from errno import *
from stat import *
from operator import itemgetter
# init logging
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
        logging.debug( '__INIT__' )
        # Initialize Fuse object
        Fuse.__init__( self, *args, **kw )
        
        # Initialize Pyro Client
        Pyro.core.initClient(0)
        
        # Instance Variables
        self.connected_master = False   # Connection status for Master
        self.connected_chunk = False    # Connection status for Chunk
        self.timestamp = 0              # Timestamp for data syncing
        self.last_offset = 0 
    ###############################################
    ### Subroutine: Connect Master Server       ###
    ###                                         ###
    ### Used by: reconnect_master_server, MAIN  ###
    ###############################################
    def connect_master_server( self ):
        logging.debug( 'CONNECT_MASTER_SERVER' )
        
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
        logging.debug( 'CONNECT_CHUNK_SERVER' )

        #if self.connected_chunk:
            #return

        # Try getting the Pyro proxy object for the Master Server    
        try:
        
            # Set protocol to use for Pyro RPC (secured or not)
            protocol = "PYRONAME://192.168.1.22/" + chunkserver_name
            # Get the master server proxy object from Pyro RPC system
            self.chunk_server = Pyro.core.getProxyForURI( protocol )
            # Check that the returned Pyro proxy object works
            if self.chunk_server.getattr('/'):
                self.connected_chunk = True
                self.timestamp = time.time()
            else:
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
        logging.debug( 'RECONNECT_MASTER_SERVER' )
        
        #toggle the master connection status to false
        self.connected_master = False
        
        #Attempt to rebind to the master server
        #self.master_server.rebindURI()
        
        #call method to connect to the master server again 
        self.connect_master_server()


    ###################################################
    ### Subroutine: Reconnect Chunk Server          ###
    ###                                             ###
    ### Used by:   rename_chunks, MushroomFile.read ###
    ###################################################
        
    def reconnect_chunk_server( self):
        logging.debug( 'RECONNECT_CHUNK_SERVER' ) 
        #release the previous lock
        
        #toggle the chunk connection status to false       
        self.connected_chunk = False

        #Attempt to rebind to the master server
        self.chunk_server.rebindURI( tries=3, wait=3 )

        
    ##########################
    ### Routine: statfs    ###
    ##########################

    def statfs( self ):
        logging.debug( 'STATFS' )
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
                logging.debug( 'EXCEPTION STATFS' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE 
        return stats
        

    ##########################
    ### Routine: getattr   ###
    ##########################

    def getattr( self, path ):
        logging.debug( 'GETATTR' )
        logging.debug( path )
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                #call to the master server to perform operation
                attr = self.master_server.getattr( path  )
                logging.debug( 'stats object: ' )
                logging.debug( attr ) 
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                logging.debug( 'EXCEPTION GETATTR' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return attr


    ##########################
    ### Routine: readlink  ###
    ##########################
        
    def readlink( self, path ):
        logging.debug( 'READLINK' )
        
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
                logging.debug( 'READLINK' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return link

        
    ##########################
    ### Routine: readdir   ###
    ##########################
        
    def readdir( self, path, offset ):
        logging.debug( 'READDIR' ) 
        #initialize operation as not successful
        successful = False
    
        #continue until we are successful
        while not successful:
            
            #Try to contact the master
            try:
                
                #call to the master server to perform iterative operations
                direntries = ['.', '..']
                logging.debug( 'trying to call master_server readdir' )
                direntries.extend( self.master_server.readdir( path[1:] ) )
                logging.debug( 'dir entries: ' )
                logging.debug( direntries )
                for item in direntries:
                    #as per FUSE docs use yield 
                    yield fuse.Direntry( item )
                
                #change operation status to successul & exit loop
                successful = True
            
            #if master is unavailable then try to reconnect
            except:
                logging.debug( 'EXCEPTION READDIR' )
                self.reconnect_master_server()
                
  
    ##########################
    ### Routine: truncate  ###
    ##########################

    def truncate( self, path, length ):
        logging.debug( 'TRUNCATE' )
    
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
                logging.debug( 'EXCEPTION TRUNCATE' )
                self.reconnect_master_server()

        #return the master's resultant to FUSE        
        return truncated_file
            
            
    ##########################
    ### Routine: rename    ###
    ##########################
        
    def rename( self, source_path, target_path  ):
        logging.debug( 'RENAME' )    
        #initialize operation as not successful
        successful = False
            
        #continue until we are successful
        while not successful:
            
            #Try1: connect master, Try2: open file
            try:
                    
                #contact master & get all this file's chunk's IDs
                chunk_ids_list = self.master_server.get_chunk_ids( source_path )
                source_dict = {}
                target_dict = {}
                #for every chunk ID for this file
                for chunk_id in chunk_ids_list:
                    #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                    uuid = chunk_id[0]
                    file_path = chunk_id[1]
                    target_name = base64.urlsafe_b64encode( target_path )
                    source_chunk_name = str( uuid ) + "--" + file_path
                    target_chunk_name = str( uuid ) + "--" + target_name
                    #contact master & using ID get the location of this chunk
                    chunk_locations_list = self.master_server.get_chunkloc( uuid )
                    for chunk_location in chunk_locations_list:
                        if chunk_location not in source_dict.keys():
                            source_dict[ chunk_location ] = [ chunk_id ]
                            target_dict[ chunk_location ] = [ (uuid, target_name) ]
                        else:
                            source_dict[ chunk_location ].append( chunk_id )
                            target_dict[ chunk_location ].append( ( uuid, target_name ) )

                successful_chunk_rename = False

                while not successful_chunk_rename:
                            
                    #Try3: connect to chunk servers
                    try:
                        #Connect to proper chunk server for this chunk
                        #Read the chunk data from chunk server
                        for chunk_location in source_dict.keys():
                            logging.debug( 'connecting to chunk server' )
                            self.connect_chunk_server( chunk_location )
                            logging.debug( 'connected to chunk sevrer' )
                            logging.debug( 'sending source' )
                            logging.debug( source_dict[ chunk_location ] )
                            logging.debug( 'sending target' )
                            logging.debug( target_dict[ chunk_location ] )
                            self.chunk_server.rename( source_dict[ chunk_location ], target_dict[ chunk_location ]  )
                            logging.debug( 'renamed on chunk servers' )
                        successful_chunk_rename = True
                            
                    #In case of chunk server connection failure, reconnect
                    except:
                        self.reconnect_chunk_server()
                        
                #change operation status to successul & exit loop
                successful = True
            
            #In case of master connection failure, reconnect & reset timestamps
            except:
                logging.debug( 'EXCEPTION RENAME' )
                client.reconnect_master_server()
                self._reinitialize()
                
        #return the read binary data to FUSE
        self.master_server.rename_chunks( source_dict, target_dict, source_path, target_path ) 

        return None

            
    ##################################
    ### Subroutine: rename chunks  ###
    ###                            ###
    ### Used by: rename            ###
    ##################################
        
    def rename_chunks( chunk_ids_list, target_path ):
        logging.debug( 'RENAME_CHUNKS' )    
    
        successful_master = False
        successful_chunk = False
        actual_renames = {}
        
        while not successful_master:
            try:
                chunk_server_list = client.master_server.get_chunk_servers()
                successful_master = True
            except:
                logging.debug( 'EXCEPTION RENAME_CHUNKS CONNECT MASTER' )
                client.reconnect_master_server()
        
        if chunk_server_list:
       
            chunk_server_index = 0
             
            while not successful_chunk:
            
                try:

                    while( chunk_ids_list ):
                        chunk_id = chunk_ids_list[0]
                        chunk_server_index = ( chunk_server_index + 1 ) % len( chunk_server_list )
                        chunk_location = chunk_server_list[ chunk_server_index ]
                        client.connect_chunk_server( chunk_location )
                        uuid = chunk_id[0]
                        new_chunk_id = ( uuid, target_path )
                        client.chunk_server.rename( chunk_id, new_chunk_id ) 
                    successful_chunk = True
                except:
                    logging.debug( 'EXCEPTION RENAME_CHUNKS CHUNK SERVER' )
                    chunk_server_index = (chunk_server_index + 1) % len( chunk_server_list )
                    self.reconnect_chunk_server( )

            return actual_renames
    
    ##########################
    ### Routine: mkdir     ###
    ##########################
            
    def mkdir( self, path, mode ):
        logging.debug( 'MKDIR' )    
    
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
                logging.debug( 'EXCEPTION MKDIR' )
                self.reconnect_master_server()
            
        #return the master's resultant to FUSE        
        return dir


    ##########################
    ### Routine: rmdir     ###
    ##########################

    def rmdir( self, path ):
        logging.debug( 'RMDIR' )    
    
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
                logging.debug( 'EXCEPTION RMDIR' )
                self.reconnect_master_server()
         
        #return the master's resultant to FUSE 
        return remove_result
    

    ##########################
    ### Routine: symlink   ###
    ##########################
        
    def symlink( self, source_path, target_path ):
        logging.debug( 'SYMLINK' )    
    
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
                logging.debug( 'EXCEPTION SYMLINK' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE 
        return symlink_result

        
    ##########################
    ### Routine: link      ###
    ##########################
        
    def link( self, source_path, target_path ):
        logging.debug( 'LINK' )    
    
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
                logging.debug( 'EXCEPTION LINK' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return link_result
        

    ##########################
    ### Routine: unlink    ###
    ##########################
        
    def unlink( self, path ):
        logging.debug( 'UNLINK' ) 
    
        #initialize operation as not successful
        successful = False
            
        #continue until we are successful
        while not successful:
            
            #Try1: connect master, Try2: open file
            try:
                    
                #contact master & get all this file's chunk's IDs
                chunk_ids_list = self.master_server.get_chunk_ids( path )
                delete_dict = {}
                #for every chunk ID for this file
                for chunk_id in chunk_ids_list:
                    #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                    uuid = chunk_id[0]
                    file_path = chunk_id[1]
                    chunk_name = str( uuid ) + "--" + file_path
                    #contact master & using ID get the location of this chunk
                    chunk_locations_list = self.master_server.get_chunkloc( uuid )
                    for chunk_location in chunk_locations_list:
                        if chunk_location not in delete_dict.keys():
                            delete_dict[ chunk_location ] = [ chunk_name ]
                        else:
                            delete_dict[ chunk_location ].append( chunk_name )

                self.master_server.deregister_chunks( path, delete_dict ) 
                successful_chunk_delete = False

                while not successful_chunk_delete:
                            
                    #Try3: connect to chunk servers
                    try:
                        #Connect to proper chunk server for this chunk
                        #Read the chunk data from chunk server
                        for chunk_location in delete_dict.keys():
                            self.connect_chunk_server( chunk_location )
                            self.chunk_server.delete( delete_dict[ chunk_location ]  )
                        successful_chunk_delete = True
                            
                    #In case of chunk server connection failure, reconnect
                    except:
                        self.reconnect_chunk_server()
                        
                #change operation status to successul & exit loop
                successful = True
            
            #In case of master connection failure, reconnect & reset timestamps
            except:
                logging.debug( 'EXCEPTION UNLINK' )
                client.reconnect_master_server()
                self._reinitialize()
                
        #return the read binary data to FUSE
        return None
        

    ##########################
    ### Routine: chmod     ###
    ##########################
        
    def chmod( self, path, mode ):
        logging.debug( 'CHMOD' )    
    
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
                logging.debug( 'EXCEPTION CHMOD' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return chmod_result
        
        
    ##########################
    ### Routine: chown     ###
    ##########################
        
    def chown( self, path, user, group ):
        logging.debug( 'CHOWN' )    
    
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
                logging.debug( 'EXCEPTION CHOWN' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return chown_result
        

    ##########################
    ### Routine: mknod     ###
    ##########################
        
    def mknod( self, path, mode, dev ):
        logging.debug( 'MKNOD' )    
    
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
                logging.debug( 'EXCEPTION MKNOD' )
                self.reconnect_master_server()
        #return the master's resultant to FUSE       
        return nod
        
        
    ##########################
    ### Routine: utime     ###
    ##########################
        
    def utime( self, path, times ):
        logging.debug( 'UTIME' )    
    
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
                logging.debug( 'EXCEPTION UTIME' )
                self.reconnect_master_server()
        
        #return the master's resultant to FUSE
        return utime_result
        
        
    ##########################
    ### Routine: access    ###
    ##########################

    def access( self, path, mode ):
        logging.debug( 'ACCESS' )    
    
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
                logging.debug( 'EXCEPTION ACCESS' )
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
            logging.debug( '__INIT__FILE' )    
        
            #set status for class init as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: to contact the master & Try2: to open file
                try:
                    
                    #call to the master server to open file on its end
                    ret = client.master_server.open( path, flags, mode )
                    attr = client.master_server.getattr( path )
                    fattr = client.master_server.fgetattr( ret, path)
                    #Use the client's timestamp for this file's timestamp
                    self.timestamp =  client.timestamp
                    
                    #Set param as this file's class instance var
                    self.path = path
                    self.flags = flags
                    self.data_store_list = []
                    self.data_store = ""
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
                    logging.debug( 'EXCEPTION __INIT__FILE CONNECT MASTER SERVER' )
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
            logging.debug( '_REINITIALIZE' )    
        
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
            logging.debug( 'GET_NUM_CHUNKS' )    
        
            # Nmbr of Chunks must be an int so must round any fraction up to next int
            return ( size + chunk_size - 1 ) // chunk_size
                
                
        """
        FILE SYSTEM ROUTINES
        """

        #############################
        ### Routine: MF.release   ###
        #############################

        def release( self, flags ):
            logging.debug( 'RELEASE' )    
            client.last_offset = 0
        
            if len( self.data_store_list ):
                self.data_store = b"".join( self.data_store_list )
                logging.debug( 'data_store is now: ' )
                logging.debug( len( self.data_store ) )
                self.write_data_store()
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
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    logging.debug( 'EXCEPTION RELEASE' )
                    client.reconnect_master_server()


        ##############################
        ### Routine: MF.ftruncate  ###
        ##############################
            
        def ftruncate( self, len ):
            logging.debug( 'FTRUNCATE' )    
        
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
                    logging.debug( 'EXCEPTION FTRUNCATE' )
                    client.reconnect_master_server()
                    self._reinitialize()
               
            #return the master's resultant to FUSE 
            return ftrunc_result


        ##############################
        ### Routine: MF.read       ###
        ##############################

        def read( self, size, offset ):
            logging.debug( 'READ' )    
            #initialize operation as not successful
            successful = False
            
            #continue until we are successful
            while not successful:
            
                #Try1: connect master, Try2: open file
                try:
                
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        logging.debug( 'timestamp failure' )
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        #init a list that holds all the chunked data segments
                        data_chunks_list = []
                        
                        #contact master & get all this file's chunk's IDs
                        chunk_ids_list = client.master_server.get_chunk_ids( self.path )
                        logging.debug( 'chunk_ids_list' )
                        logging.debug( chunk_ids_list )
                        #sort the chunk IDS such that they are in order
                        sorted_chunk_ids_list = sorted( chunk_ids_list, key=itemgetter( 0 ) )

                        chunk_size = client.master_server.get_chunk_size()
                        #call to subroutine, returns the # of chunks to split data into
                        previous_num_chunks = self.get_num_chunks( client.last_offset, chunk_size )
                        num_chunks = self.get_num_chunks( size, chunk_size )
                        client.last_offset = client.last_offset + size

                        #for every chunk ID for this file
                        for chunk_id in sorted_chunk_ids_list[previous_num_chunks:num_chunks+previous_num_chunks]:
                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            uuid = chunk_id[0]
                            file_path = chunk_id[1]
                            chunk_name = str( uuid ) + "--" + file_path
                            #contact master & using ID get the location of this chunk
                            chunk_locations_list = client.master_server.get_chunkloc( uuid )
                            logging.debug( 'got chunk_location_list' )
                            logging.debug( chunk_locations_list )
                            successful_chunk_read = False
                            #chunk_location_index = 0

                            while not successful_chunk_read:
                            
                                #master returns a list of servers with that chunk, pick the 1st
                                chunk_location = chunk_locations_list[0]
                                logging.debug( 'chunk_location' )
                                logging.debug( chunk_location )
                                #Try3: connect to chunk servers
                                try:
                                    #Connect to proper chunk server for this chunk
                                    logging.debug( 'trying to connect to chunk server' )
                                    client.connect_chunk_server( chunk_location )
                                    logging.debug( 'connected to chunk server' )
                                    #Read the chunk data from chunk server
                                    data_chunk = client.chunk_server.read( chunk_name )
                                    logging.debug( 'got data_chunk length: ' )
                                    logging.debug( len( data_chunk ) )
                                    #add this chunk's data to the list
                                    data_chunks_list.append( data_chunk )
                                    successful_chunk_read = True
                            
                                #In case of chunk server connection failure, reconnect
                                except:
                                    logging.debug( 'EXCEPTION READ CHUNK SERVER' )
                                    #chunk_location_index = ( chunk_location_index + 1 ) % len( chunk_location )
                                    chunk_locations_list.append( chunk_locations_list.pop(0) )
                                    #client.reconnect_chunk_server()
                        
                        #convert file data into binary data
                        data = b"".join( data_chunks_list )
                
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of master connection failure, reconnect & reset timestamps
                except:
                    logging.debug( 'EXCEPTION READ' )
                    client.reconnect_master_server()
                    self._reinitialize()
                
            #return the read binary data to FUSE
            return data
            

        def write( self, buf, offset ):
            logging.debug( 'WRITE' )

            logging.debug( 'appending to data_store_list' )
            self.data_store_list.append( buf )
            logging.debug( 'data_store_list is now: ' )
            logging.debug( len( self.data_store_list ) )

            return len( buf )


        ###############################
        ### Routine: MF.write       ###
        ###############################

        def write_data_store( self ):
            #initialize operation as not successful
            successful = False
            actual_writes = {}
            #continue until we are successful
            while not successful:
                #Try for connection to master
                try:
                    
                    #Verify that the file's timestamp is valid
                    if( self.timestamp != client.timestamp ):
                        #And reinit so that timestamp gets updated
                        self._reinitialize()
                    
                    #Otherwise the file's timestamps do match & therefore:
                    else:
                        #if the file already exists on master then overwrite it 
                        #if client.master_server.exists():
                        #    client.ftruncate( self.path  )
                         
                        #contact master and get the chunk size 
                        chunk_size = client.master_server.get_chunk_size()
                        #call to subroutine, returns the # of chunks to split data into
                        num_chunks = self.get_num_chunks( len( self.data_store ), chunk_size )
                        #contact master to generate a unique id for each chunk
                        chunk_ids_list = client.master_server.generate_chunk_ids( self.file_descriptor, self.path, num_chunks, len( self.data_store ) )
                        #call to subroutine to write each chunk to the appropriate chunk server
                        actual_writes = self.write_chunks( chunk_ids_list, chunk_size )
                        logging.debug( 'in write, actual_writes' )
                        logging.debug( actual_writes )
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of master connection failure, reconnect & reset timestamps
                except:
                    logging.debug( 'EXCEPTION WRITE MAIN TRY' )
                    client.reconnect_master_server()
                    self._reinitialize()
            
            #After successful write, confirm with master which chunkserver's belong with each ID
            successful_confirm = False

            while not successful_confirm:
                try:
                    client.master_server.register_chunks( actual_writes, self.path )
                    successful_confirm = True
                except:
                    logging.debug( 'EXCEPTION WRITE REGISTER CHUNKS' )
                    client.reconnect_master_server()

            
        ######################################
        ### Subroutine: MF.write_chunks    ###
        ###                                ###
        ### Used by:   MF.write            ###
        ######################################
            
        def write_chunks(self, chunk_ids_list, chunk_size ):
            logging.debug( 'WRITE_CHUNKS' )    
        
            #writing status with master
            successful_master = False
            
            #writing status with chunk servers
            successful_chunk = False
            #splice original data into chunks where size of chunks defined by master
            data_chunks_list = [ self.data_store[index:index + chunk_size] for index in xrange(0, len( self.data_store ), chunk_size ) ]
            #init dict holding chunks ids & servers that have already successfully written (in case of fail)
            actual_writes = {}
            chunk_server_list = [] 
            #continue until we are successful & done with master
             
            while not successful_master:
                
                #Try for connection to master
                try:
                    #Get a full list of active chunk servers from the master
                    chunk_server_list = client.master_server.get_chunk_servers()
                    logging.debug( 'chunk_server_list' )
                    logging.debug( chunk_server_list )
                    #finished with master, move to next step
                    successful_master = True
                
                #In case of master connection failure then try to reconnect
                except Exception:
                    logging.debug( 'EXCEPTION WRITE_CHUNKS MASTER SERVER' )
                    client.reconnect_master_server()
            #Next, as long as we have chunk servers then try to write chunks to them
            
            if chunk_server_list:
            
                #continue until we are successful & done with writing all chunks
                while not successful_chunk:
                
                    #Try to write all chunks to the chunk servers using round robin (from list)
                    try:
                        #for each chunk of data
                        for index in xrange( 0, len( data_chunks_list ) ):
                            #get the key that associates to that chunk
                            chunk_id = chunk_ids_list[ index ]
                            #Chunk IDs are tuples:(TimeUUID, path);combine them for filename
                            uuid = str( chunk_id[0] )
                            file_path = chunk_id[1]
                            chunk_name = uuid + "--" + file_path
                            #Change which chunk server will get the next chunk (cycles through chunk severs)
                            chunk_server_index = index % len( chunk_server_list )

                            actual_writes[ chunk_id ] = []
                            #from list get location of where chunk should go (i.e. which chunkserver)
                            logging.debug( 'getting chunk_location ' )
                            chunk_location = chunk_server_list[ chunk_server_index ]
                            logging.debug( chunk_location )
                            #connect to that chunk server
                            logging.debug( 'connecting to chunk server' )
                            client.connect_chunk_server( chunk_location )
                            logging.debug( 'connected to chunk server' )
                            #write that chunk data to the chunk server
                            logging.debug( 'writing to chunk server' )
                            client.chunk_server.write( data_chunks_list[ 0 ], chunk_name )
                            actual_writes[ chunk_id ].append ( chunk_location )
                            logging.debug( 'wrote to chunk server' )
                            
                            replication_one_index = ( chunk_server_index + 1) % len( chunk_server_list )
                            chunk_location = chunk_server_list[ replication_one_index ]
                            logging.debug( 'second write to chunk_location' )
                            logging.debug( chunk_location )
                            #connect to that chunk server
                            client.connect_chunk_server( chunk_location )
                            #write that chunk data to the chunk server
                            client.chunk_server.write( data_chunks_list[ 0 ], chunk_name )
                            actual_writes[ chunk_id ].append( chunk_location )

                            replication_two_index = ( chunk_server_index + 2) % len( chunk_server_list )
                            chunk_location = chunk_server_list[ replication_two_index ]
                            #connect to that chunk server
                            client.connect_chunk_server( chunk_location )
                            #write that chunk data to the chunk server
                            client.chunk_server.write( data_chunks_list[ 0 ], chunk_name )
                            actual_writes[ chunk_id ].append( chunk_location )

                            #delete that chunk from list (in case of failure: only failed chunks retry)
                            del data_chunks_list[ 0 ]
                            
                        #finished with writing  
                        successful_chunk = True
                    
                    #In case of failure due to inactive server, then remove that server from list & try again
                    
                    except Exception, error:
                        logging.debug( 'EXCEPTION WRITE_CHUNKS WRITING CHUNKS' )
                        #del chunk_server_list[ chunk_server_index ]
             
            #return the dict back to write method
            return actual_writes
                
                
        ###############################
        ### Routine: MF.fgetattr    ###
        ###############################

        def fgetattr( self ):
            logging.debug( 'FGETATTR' )    
        
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
                        attr = client.master_server.fgetattr( self.file_descriptor, self.path )
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    logging.debug( 'EXCEPTION FGETATTR' )
                    client.reconnect_master_server()
                    self._reinitialize()
                
            #return the master's resultant to FUSE        
            return attr
                
                
        ###############################
        ### Routine: MF.flush       ###
        ###############################

        def flush( self ):
            logging.debug( 'FLUSH' )    
        
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
                        flush_result = client.master_server.flush( self.file_descriptor )
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    logging.debug( 'EXCEPTION FLUSH' )
                    client.reconnect_master_server()
                    self._reinitialize()
                    
            #return the master's resultant to FUSE 
            return flush_result 

                
        ###############################
        ### Routine: MF.fsync       ###
        ###############################
            
        def fsync( self, isfsyncfile ):
            logging.debug( 'FSYNC' )    
        
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
                        fsync_result = client.master_server.fsync( self.file_descriptor, isfsyncfile )
                        
                        #change operation status to successul & exit loop
                        successful = True
            
                #In case of connection failure, try to reconnect to master using client
                except:
                    logging.debug( 'EXCEPTION FSYNC' )
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

    client.fuse_args.add( 'big_writes' )
    client.fuse_args.add( 'large_read' )
    
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
        client.main()
    except Exception, error:
        # Unmount the filesystem.
        if client.parser.fuse_args.mountpoint:
            print >> sys.stderr,"umounting " + str( client.parser.fuse_args.mountpoint )

if __name__ == '__main__':
        main()
