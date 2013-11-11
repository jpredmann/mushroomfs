
import os, sys, threading, time
from errno import *
from stat import *

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
        
    def connect( self ):
        
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
                
    def reconnect( self ):
    
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
		
	def symlink( self, source_path, target_path ):
	
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
                
                except:
                    client.reconnect()
                    
            if self.file_descriptor < 0:
                raise OSError( errno.EACCES, "Premission denied: " + self.path )
                
        def _reinitialize( self ):
        
            if hasattr( self, 'mode' ):
                self.__init__( self.path, self.flags, self.mode ) 
                
            else:
                self.__init__( self.path, self.flags )
                
                
        """
        FILE SYSTEM ROUTINES
        """
        
        