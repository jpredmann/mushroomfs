import math
import time_uuid    #Used to generate unique chunk ids/keys
import os           #Used to write data out
import time         #Used to create timestamp for archiving deleted files
import operator     #Used in a Master 'tester method' (Server.dump_metadata())
import sys
import argparse

try:
	import Pyro.core
except:
	print >> sys.stderr, """
error: Pyro framework doesn't seem to be correctly installed!

Follow the instruction in the README file to install it, or go the Pyro
webpage http://pyro.sourceforge.net.
"""
	sys.exit(1)

# Certificate validator class.
class MushroomCertValidator(Pyro.protocol.BasicSSLValidator):
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

	def authorize(self, subject):
		return (1, 0)

	def deny(self, subject, code):
		return (0, code)

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


class MushroomMaster(Pyro.core.ObjBase):

    def __init__(self):
		
		print "I am here"
		self.root = os.path.abspath( root ) + '/'
		os.chdir( self.root )
		self.chunksize = 1048576		# Max size in megabytes of chunks
		self.chunkrobin = 0				# Index of the next chunk server to use
		self.file_table = {}			# Look-up table to map from file paths to chunk ids
		self.chunk_table = {} 			# Look-up table to map chunk id to chunk server
		self.chunk_server_table = {}	# Look-up table to map chunk servers to chunks held
		self.chunk_servers = []		# List of registered chunk servers
		Pyro.core.ObjBase.__init__( self )
        
        
    # Make an dictionary of empty lists where keys are chunk servers and values are
    # a list of chunk ids stored on that chunk server
    def init_chunk_server_table():
    
    	for server in self.chunk_servers:
    		self.chunkservertable[ server ] = []
    		
	# Returns a list of chunk servers considered by the master server to be
	# currently available.  This list get updated by the master server when 
	# Zookeeper reports that a chunk server is no longer available.
    def get_chunk_servers(self):
    
        return self.chunk_servers

	# Returns a list of chunk ids.  Calls an internal chunk allocation method to
	# perform the 'house keeping' tasks with the meta-data tables.
    def alloc(self, path, num_chunks): # return ordered chunkuuid list
    
    	# Call to house keeping subroutine
        chunk_ids = self.alloc_chunks(num_chunks, path)
        
        # Adds file path to the file table, if it was not present already
        # stores, potentially over-writing, a list of chunk ids, where those chunks
        # compose the file.
        self.file_table[path] = chunk_ids
        
        return chunk_ids

	# Internal house keeping method to update meta-data tables, returns a list of chunk
	# ids back to the allocating method that called it.
    def alloc_chunks(self, num_chunks, path):
    
        chunk_ids = []					# List to hold chunk ids
        
        # Iterate over the number of chunks the file has been split into
        for i in range(0, num_chunks):
        
        	# Generate a new UUID for the current chunk
            chunkuuid = uuid.uuid1()
            # Get the index of the chunk server list that holds the next server to use
            chunkloc = self.chunkrobin
            # Add entry into the chunk table for the new UUID containing primary server
            # for that chunk (pre-replication).
            self.chunk_table[chunkuuid] = [ self.chunk_servers[chunkloc] ]
            # Append to the entry in the chunk server table the chunk id that is now held
            # on that chunk server.
            self.chunk_server_table[ self.chunk_servers[chunkloc] ].append( (chunkuuid, path) ) 
            # add the new chunk id--consisting of a UUID, path pair, to the chunk id list
            chunk_ids.append( (chunkuuid, path) )
            # Update the round-robin to the next chunk server
            self.chunkrobin = (self.chunkrobin + 1) % len( self.chunk_servers )
            
        return chunk_ids
        
    # Allocation method for appending to a file
    def alloc_append(self, path, num_append_chunks): # append chunks
    
    	# Get chunk ids for existing chunks in file
        chunk_ids = self.file_table[path]
    	# Call to house keeping subroutine to get chunk ids for chunks being appended
        append_chunk_ids = self.alloc_chunks(num_append_chunks, path)
        # Append chunk ids to list of existing chunks
        chunk_ids.extend(append_chunk_ids)
        
        return append_chunk_ids

	# Get the list of chunk servers that hold the given chunk
    def get_chunkloc(self, chunk_id):
    
        return self.chunk_table[chunk_id]

	# Get the list of ids of the chunks that compose the given file
    def get_chunk_ids(self, path):
    
        return self.file_table[path]

	# Determine if the file already exists
    def exists(self, path):
    
        return True if path in self.file_table else False

	
    def delete(self, path): # rename for later garbage collection
    
        chunk_ids = self.file_table[path]
        
        for id in chunk_ids:
        	for server in self.chunk_table[ id ]:
        		self.chunk_server_table[server].remove( id )
        	
        	del self.chunk_table[id]
        
        del self.file_table[path]
       

    def register_chunk_server(self, ip_address, port_number):
    
    	self.chunk_servers.append( (ip_address, port_number) )
            
            
    # Need to implement a replace chunk server method that can be called by Zookeeper
    # the call will provide the ip and port of a replacement chunk server
    
	def statfs( self ):
	
		try:
			op_result =  os.statvfs( self.root )
		except:
			op_result = -errno.EACCES
			
		return op_result



	def getattr( self, path ):
	
		try:
			op_result = os.lstat( self.root + path )
		except:
			op_result = -errno.ENOENT
			
		return op_result

	def readlink( self, path ):
	
		try:
			op_result =  os.readlink( self.root + path )
		except:
			op_result = -errno.ENOENT
			
		return op_result

	def readdir( self, path ):
	
		try:
			op_result = os.listdir( self.root + path )
		except:
			op_result = -errno.EBADF
			
		return op_result

	def open( self, path, flags, mode ):
	
		# if successful the op_result holds the file descriptor
		try:
			if mode:
			    op_result = os.open( self.root + path, flags, mode[0] )
			else:
				op_result = os.open( self.root + path, flags )
				
		# if not successful op_result holds the error code		
		except:
			op_result -errno.ENOENT
			
		return op_result

	# TODO: Determine what if anything should be returned here
	def release( self, file_descriptor, flags ):
	
		try:
			if file_desciptor > 0:
				os.close( file_descriptor )
				op_result = True
		except:
			op_result =  -errno.ENOSYS
			
		return op_result
			

	def ftruncate( self, file_descriptor, length ):
	
		try:
			op_result = os.ftruncate( file_descriptor, length )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def read( self, file_descriptor, size, offset ):
	
		try:
			os.lseek( file_descriptor, offset, 0 )
			op_result = os.read( file_descriptor, size )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def write( self, file_descriptor, buffer, offset ):
	
		try:
			os.lseek( file_descriptor, offset, 0 )
			os.write( file_descriptor, buffer )
			op_result = len( buffer )
		except:
			op_result -errno.EACCES
		
		return op_result

	def fgetattr( self, file_descriptor ):
	
		try:
			op_result = os.fstat( file_descriptor )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def flush( self, file_descriptor ):
	
		try:
			op_result = os.close( os.dup( file_descriptor ) )
		except:
			op_result = -errno.EACCES
			
		return op_result

	# TODO: determine what should be returned
	def fsync( self, file_descriptor, isfsyncfile ):
	
		try:
			if isfsyncfile and hasattr(os, 'fdatasync'):
				os.fdatasync( file_descriptor )
			else:
				os.fsync( file_descriptor )
		except:
			return -errno.EACCES

	# TODO: return issues
	def truncate( self, path, length ):
	
		try:
			file = open( self.root + path, 'ab' )
			file.truncate( length )
			file.close()
		except:
			return -errno.EACCES

	def mkdir(self, path, mode):
	
		try:
			op_result = os.mkdir( self.root + path, mode )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def rmdir( self, path ):
	
		try:
			op_result = os.rmdir( self.root + path )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def symlink( self, source_path, target_path ):
	
		try:
			op_result = os.symlink( source_path, self.root + target_path )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def link( self, source_path, target_path ):
	
		try:
			op_result = os.link( source_path, self.root + target_path )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def unlink( self, path ):
	
		try:
			op_result = os.unlink( self.root + path )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def rename( self, source_path, target_path ):
	
		try:
			op_result = os.rename( self.root + source_path, self.root + target_path )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def chmod( self, path, mode ):
	
		try:
			op_result = os.chmod( self.root + path, mode )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def chown(self, path, user, group):
		try:
			return os.chown(self.root + path, user, group)
		except:
			return -errno.EACCES

	def mknod( self, path, mode, device ):
	
		try:
			op_result = os.mknod( self.root + path, mode, device )
		except:
			op_result = -errno.EACCES
			
		return op_result

	def utime( self, path, times ):
	
		try:
			op_result = os.utime( self.root + path, times )
		except:
			op_result = -errno.EACCES
			
		return op_result

	# TODO: This looks incorrect, look into it
	def access( self, path, mode ):
	
		try:
			if not os.access( self.root + path, mode ):
				raise
		except:
			return -errno.EACCES


    
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
	#parser.add_argument( '-v', '--version', action='store_true', help="Prints version information for the chunk server" )
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
		
		if secure:
			daemon = Pyro.core.Daemon(prtcol='PYROSSL', host=hostname, port=port)
			daemon.setNewConnectionValidator(MushroomCertValidator())
		else:
			daemon = Pyro.core.Daemon(prtcol='PYRO', host=hostname, port=port)
			
		# Use persistent connection (we don't want to use a Pyro
		# nameserver, to keep the things simple).
		uri = daemon.connectPersistent(master_server, 'MushroomFS')
		
		try:
			# Start the daemon.
			daemon.requestLoop()
		except:
			# Shutdown the daemon.
			daemon.shutdown(True)

if __name__ == '__main__':
	main()


            