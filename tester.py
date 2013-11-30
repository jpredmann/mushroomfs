import os

fd = os.open( '/home/groupd/client/test.txt', os.O_CREAT|os.O_RDWR )
os.write( fd, "testing test" )
os.close( fd )
