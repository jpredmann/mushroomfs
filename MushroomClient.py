
import os, sys
from errno import *
from stat import *

# Try importing Fuse, generate an error message and exit if it cannot import Fuse
# Using Fuse-Python 0.2
try:
    import fuse
    from fuse import Fuse, FuseArgs
except ImportError:
	print >> sys.stderr, "Fuse does not appear to be properly installed"
	sys.exit(1)
	
# Try importing Pyro, generate an error message and exit if it cannot import Pyro
# Using Pyro version 3.16
try:
	import Pyro.core
	import Pyro.protocol
except ImportError:
	print >> sys.stderr, "Pyro does not appear to be properly installed"
	sys.exit(1)
	
	
# Initialize the fuse api environment
# Set fuse version number
fuse.fuse_python_api = ( 0, 2 )
# ??
fuse.feature_assert('stateful_files')


# Generate an error if the fuse api cannot interact with the Fuse kernel module
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."



class MushroomClient(FUSE):

    def __init__(self, *args, **kw):
    
        Fuse.__init__(self, *args, *kw)
        