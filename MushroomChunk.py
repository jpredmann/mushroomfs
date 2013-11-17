#!/usr/bin/env python

import sys, os, errno, getopt, logging, signal, re, argparse
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


# The chunk server class.
class MushroomChunk( Pyro.core.ObjBase ):
	def __init__( self, root ):
		self.root = os.path.abspath( root ) + '/'
		os.chdir( self.root )
		Pyro.core.ObjBase.__init__( self )

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
			if fd > 0:
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
	port = DEFAULT_PORT
	foreground = False
	secure = False

	parser = argparse.ArgumentParser()

	parser.add_argument( 'mount_point', dest=mount_point, help="A directory or mount point must be specified" )
	parser.add_argument( '-v', '--version', action='store_true', help="Prints version information for the chunk server" )
	parser.add_argument( '-a', '--address', dest=hostname, help="Used to specify the ip address of the chunk server" )
	parser.add_argument( '-p', '--port', dest=port, help="User to specify the port the chunk server listens on" )
	parser.add_argument( '-s', '--secure', action='store_true', dest=foreground, help="Run in secure mode using x509 certificate authentication" )
	parser.add_argument( '-f', '--foreground', action='store_true', dest=secure, help="Run the chunk server in the foreground" )

	parser.parse_args()

	# Instantiate chunk server
	try:
		chunk_server = MushroomChunk( mount_point )
	except:
		print >> sys.stderr, "Error: could not mount " + mount_point
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
		uri = daemon.connectPersistent(chunk_server, 'MushroomFS')
		
		try:
			# Start the daemon.
			daemon.requestLoop()
		except:
			# Shutdown the daemon.
			daemon.shutdown(True)

if __name__ == '__main__':
	main()

