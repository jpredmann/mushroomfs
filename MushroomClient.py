
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

# Mushroom client class, inherits from class Fuse
class MushroomClient(Fuse):
"""
This class will process all system calls received by the Fuse module
"""

    def __init__( self, *args, **kw ):
    
    	# Initialize Fuse object
        Fuse.__init__( self, *args, *kw )
        
        # Initialize Pyro
        Pyro.core.initClient(0)
        
        # Instance Variables
        
        # TODO: Document these instance variables
        self.lock = threading.Lock()
        self.host = '127.0.0.1'
        self.port = 3636
        self.authentication_on = False
        self.connected = False
        self.timestamp = 0
        
        
    # TODO: Make parametric to connect to either chunk or master servers
    def connect_master_server( self ):
        
        # Get a lock to talk to the server, if a client thread is already talking to
        # the server release the lock
        self.lock.acquire()
        
        # TODO: This code and the lock acquire are ugly, needs to be replaced
        if self.connected:
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
            self.master_server = Pyro.core.getProxyForURI( protocol + self.host + ":" + str(self.port) + "/PyGFS" )
            
            # Check that the returned Pyro proxy object works
            if self.master_server.getattr('/'):
                self.connected = True
                self.timestamp = time.time()
                
            else:
                raise
                
        except Exception, error:
            print str(error)
            
        # Release the lock so that other threads can acquire it
        self.lock.release()
        
    # TODO: Make parametric to connect to either chunk or master servers
    def connect_chunk_server( self, ip, port ):
        
        # Get a lock to talk to the server, if a client thread is already talking to
        # the server release the lock
        self.lock.acquire()
        
        # TODO: This code and the lock acquire are ugly, needs to be replaced
        if self.connected:
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
            self.chunk_server = Pyro.core.getProxyForURI( protocol + str(ip) + ":" + str(port) + "/PyGFS" )
            
            # Check that the returned Pyro proxy object works
            if self.master_server.getattr('/'):
                self.connected = True
                self.timestamp = time.time()
                
            else:
                raise
                
        except Exception, error:
            print str(error)
            
        # Release the lock so that other threads can acquire it
        self.lock.release()
                
    def reconnect_master_server( self ):
    
        try:
            self.lock.release()
        except:
            pass
            
        self.lock.acquire()
        self.connected = False
        self.lock.release()
        
        self.master_server.rebindURI()
        
        self.connect()
        
	def statfs( self ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				stats = self.master_server.statfs()
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return stats
		
	def getattr( self, path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				chunk_ids = self.master_server.get_chunk_ids( path )
				for id in chunk_ids:
					location = self.master_server.get_chunkloc( id )
					ip = location[0]
					port = location[1]
					connect_chunk_server( ip, port )
					self.chunk_server.get_chunk
				attr = self.master_server.getattr( path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return attr
		
	def readlink( self, path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				link = self.master_server.readlink( path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return link
		
	def readdir( self, path, offset ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				for item in self.master_server.readdir( path, offset )
				    yield fuse.Direntry( item )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		
	def truncate( self, path, length ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				truncated_file = self.master_server.truncate( path, length )
				self.lock.release()
				
				successful = True
			except:
				self.reconnect()
				
		return truncated_file
		
	def rename( self, source_path, target_path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				renamed_file = self.master_server.rename( source_path, target_path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return renamed_file
            
	def mkdir( self, path, mode ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				dir = self.master_server.mkdir( path, mode )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return dir
		
	def rmdir( self, path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				remove_result = self.master_server.rmdir( path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return remove_result
		
		
	def symlink( self, source_path, target_path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				symlink_result = self.master_server.symlink( source_path, target_path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return symlink_result
		
	def link( self, source_path, target_path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				link_result = self.master_server.link( source_path, target_path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return link_result
		
	def unlink( self, path ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				unlink_result = self.master_server.unlink( path )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return unlink_result
		
	def chmod( self, path, mode ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				chmod_result = self.master_server.chmod( path, mode )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return chmod_result
		
	def chown( self, path, user, group ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				chown_result = self.master_server.chown( path, user, group )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return chown_result
		
	def mknod( self, path, mode, dev ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				nod = self.master_server.mknod( path, mode, dev )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return nod
		
	def utime( self, path, times ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				utime_result = self.master_server.utime( path, times )
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return utime_result
		
	def access( self, path, mode ):
	
	    successful = False
	
		while not successful:
			try:
			    # TODO: Add calls to chunk servers
				self.lock.acquire()
				access_result = self.master_server.access( path, mode)
				self.lock.release()
				
				successful = True
				
			except:
				self.reconnect()
				
		return access_result


    class MushroomFile():
    
        def __init__( self, path, flags, *mode ):
        
            successful = False
            
            while not successful:
            
            	try:
                    client.lock.acquire()
                    self.file_desctriptor = client.master_server.open( path, flags, mode	)
                    self.timestamp =  client.timestamp
                    client.lock.release()
                
                    self.path = path
                    self.flags = flags
                
                    if mode:
                        self.mode = mode[0]
                    
                    successful = True;
                    
                except FileNotFoundException error:
                	raise error
                
                except:
                    client.reconnect()
                    
            if self.file_descriptor < 0:
                raise OSError( errno.EACCES, "Premission denied: " + self.path )
                
        def _reinitialize( self ):
        
            if hasattr( self, 'mode' ):
                self.__init__( self.path, self.flags, self.mode ) 
                
            else:
                self.__init__( self.path, self.flags )
                
        def get_num_chunks( size, chunk_size ):
        
        	return ( size + chunk_size - 1 ) // chunk_size
                
                
        """
        FILE SYSTEM ROUTINES
        """
        
        def release( self, flags ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        successful = True
                    else:
                        client.master_server.release( self.file_descriptor, flags )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    
			
        def ftruncate( self, len ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                        ftrunc_result = client.master_server.ftrucate( self.file_descriptor, len )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    self._reinitialize()
                    
            return ftrunc_result


        def read( self, size, offset ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                    	data_chunks = []
                    	
                    	chunk_ids = client.master_server.get_chunk_ids()
                    	sortec_chunk_ids = sorted( chunk_ids, key=itemgetter( 0 ) )
                    	
                    	for id in sorted_chunk_ids:
                    	
                    		chunk_name = str( id[0] ) + id[1]
                    		chunk_location = client.master_server.get_chunkloc( chunk_name )
                    		ip = chunk_location[0]
                    		port= chunk_location[1]
                    		client.connect_chunk_server( ip, port )
                    		data_chunks.append( client.chunk_server.read( chunk_name )
                    		
                    	data = b"".join( data_chunks )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    self._reinitialize()
                    
            return data
 

        def write( self, buffer, offset ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                    
                    	if client.master_server.exists():
                    		client.delete( self.path )
                    		
                    	chunk_size = client.master_server.get_chunk_size()
                    	num_chunks = client.get_num_chunks( len( buffer), chunk_size )
                    	chunk_ids = client.master_server.alloc( self.path, num_chunks )
                    	self.write_chunks( chunk_ids, buffer, chunk_size )
                    	
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect_master_server()
                    self._reinitialize()
                    
            return write_result
            
        def write_chunks( chunk_ids, buffer, chunk_size ):
        
        	successful_master = False
        	successful_chunk = False
        	chunks = [ buffer[index:index + chunk_size] for index in range(0, len( buffer ), chunk_size ) ]
        	actual_writes = {}
        	
        	while not successful_master:
        	
        	    try:
        			chunk_server_list = client.master_server.get_chunk_servers()
        			successful_master = True
        		except:
        			client.reconnect_master_server()
        	
        	while not successful_chunk:
        	

        			
        		if chunk_server_list:	
        			try:
        	
        				for index in range( 0, len( chunks ) ):
        					chunk_id = chunk_ids[ index ]
        					chunk_location = chunk_server_list[ index]
        					ip = chunk_location[0]
        					port = chunk_location[1]
        					client.connect_chunk_server( ip, port )
        					client.chunk_server.write( chunks[ index ] )
        					actual_writes[ chunk_id ] = chunk_location
        				successful_chunk = True
        			except:
        				del chunk_server_list[ index ]
        		


        def fgetattr( self ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                        attr = client.master_server.fgetattr( self.file_descriptor )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    self._reinitialize()
                    
            return attr

			
        def flush( self ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                        flush_result = client.master_server.flush( self.file_descriptor )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    self._reinitialize()
                    
            return flush_result 

		def fsync(self, isfsyncfile):
			while 1:
				try:
					fuse_server.synlock.acquire()
					if (self.timestamp != fuse_server.timestamp):
						fuse_server.synlock.release()
						self._reinitialize()
						continue
					ret = fuse_server.server.fsync(self.file_descriptor, isfsyncfile)
					fuse_server.synlock.release()
					break
				except:
					fuse_server.exception_handler()
					self._reinitialize()
			return ret
			
        def fsync( self, isfsyncfile ):
        
            successful = False
            
            while not successful:
            
                try:
                
                    client.lock.acquire()
                    if( self.timestamp != client.timestamp ):
                        client.lock.release()
                        self._reinitialize()
                    else:
                        fsync_result = client.master_server.fsync( self.file_descriptor, isfsyncfile )
                        client.lock.release()
                        successful = True
                except:
                    client.reconnect()
                    self._reinitialize()
                    
            return fsync_result
            
    def main( self, *file_args, **kw ):
        
        self.file_class = self.MushroomFile
        return Fuse.main(self, *file_args, **kw )
        
class FileNotFoundException( Exception ):

	pass
            

# Main program.
def main():
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
			while client.connected == 0:
				client.connect()
				time.sleep(1)
	try:
		# Mount the filesystem.
		client.main()
	except Exception, error:
		# Unmount the filesystem.
		if client.parser.fuse_args.mountpoint:
			print >> sys.stderr,"umounting " + str( client.parser.fuse_args.mountpoint )

if __name__ == '__main__':
        main()