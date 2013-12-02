#!/usr/bin/env python

import sys, os, errno, getopt, logging, signal, re, argparse
import netifaces

try:
    import Pyro.core, Pyro.naming
except:
    print >> sys.stderr, """
error: Pyro framework doesn't seem to be correctly installed!

Follow the instruction in the README file to install it, or go the Pyro
webpage http://pyro.sourceforge.net.
"""
    sys.exit(1)

# The chunk server class.
class MushroomChunk( Pyro.core.ObjBase ):
    def __init__( self, root ):
        self.root = os.path.abspath( root ) + '/'
        os.chdir( self.root )
        Pyro.core.ObjBase.__init__( self )

    def getattr( self, path ):
    
        try:
            op_result = os.lstat( self.root + path )
        except:
            op_result = -errno.ENOENT
            
        return op_result            

    def read( self, path ):
    
        try:
            with open(path, "rb") as file:
                op_result = file.read()
        except:
            op_result = -errno.EACCES
            
        return op_result

    def write( self, data, path ):
    
        try:
            with open( self.root + path, 'w' ) as file:
                file.write( data )
            op_result = len( data )
        except:
            op_result = -errno.EACCES
        
        return op_result


    def rename( self, source_list, target_list ):
    
        try:
            for source_chunk_id, target_chunk_id in zip( source_list, target_list ):
                source_path = str( source_chunk_id[0] ) + "--" + source_chunk_id[1]
                target_path = str( target_chunk_id[0] ) + "--" + target_chunk_id[1]
                op_result = os.rename( self.root + source_path, self.root + target_path )
        except:
            op_result = -errno.EACCES
            
        return op_result


    def utime( self, path, times ):
    
        try:
            op_result = os.utime( self.root + path, times )
        except:
            op_result = -errno.EACCES
            
        return op_result

    def access( self, path, mode ):
    
        try:
            if not os.access( self.root + path, mode ):
                raise
        except:
            return -errno.EACCES

    def delete( self, delete_list ):
        
        try:
        
            for chunk_name in delete_list:
                os.unlink( self.root + chunk_name )
        except:
            return -errno.EACCES

    def readdir( self ):
        file_size_list = []
        dir_list = os.listdir( self.root )
        recovery_data = {}

        stripped_dir_list = [ item for item in dir_list if item[0] != '.' ]
        for item in stripped_dir_list:
            file_size = os.path.getsize( self.root + item )
            file_size_list.append( file_size )

        recovery_data[ 'size'] = file_size_list
        recovery_data[ 'files' ] = dir_list

        return recovery_data
        

# Main program.
def main():

    mount_point = '/home/groupd/chunk'
    parser = argparse.ArgumentParser()

    parser.add_argument( 'mount_point', help="A directory or mount point must be specified" )
    parser.add_argument( 'server_name', help="The name used by Pyro to identify this chunk server" )

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

        #get Pyro name server
        ns=Pyro.naming.NameServerLocator().getNS(host='192.168.1.22')
        
        ip = netifaces.ifaddresses('eth0')[2][0]['addr']
        daemon = Pyro.core.Daemon('PYRO', ip)
        daemon.useNameServer(ns)
            
        uri = daemon.connect( chunk_server, server_name )
        
        try:
            # Start the daemon.
            daemon.requestLoop()
        except:
            # Shutdown the daemon.
            daemon.shutdown(True)

if __name__ == '__main__':
    main()

