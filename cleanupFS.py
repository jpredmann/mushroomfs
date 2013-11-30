import subprocess
import os
import sys

#Script requires argument -> "Server", "Chunk", "Client" 
server_type  = sys.argv[1]

#Captialize entire string so case in not an issue
server_type = server_type.upper()

#Cleanup Client server
if server_type == "CLIENT":

    #change to proper directory
    os.chdir('/home/groupd/')

    #unmount with fuse
    subprocess.call(["fusermount","-u","/home/groupd/client"])

#Kill the Master/Chunk services
if server_type == "MASTER" or "CHUNK":

    #get running processes as output from system call
    p = subprocess.Popen(["ps"], stdout=subprocess.PIPE)
    out, err = p.communicate()

    #convert output into a pythonic list of processes
    processes_output = out.split('\n')
    processlist = []
    for line in processes_output:
        line = line.split()
        #remove garbage lines from output 
        if len(line) == 4:
            processlist.append(line)

    #get the python processes
    pyprocesses = [ pid for (pid, tty, time, cmd) in processlist if "PYTHON" in cmd.upper() ]

    #get this pythons process id
    this_PID = str(os.getpid() )
    pyprocesses.remove(this_PID)

    #kill the python processes
    for process in pyprocesses:
        subprocess.call(['kill', process])

#Clean up Master server
if server_type == "MASTER":
    #remove server files
    serverpath = '/home/groupd/server/'
    for serverFile in os.listdir(serverpath):
        os.remove(serverpath + serverFile)

#Cleanup chunk server
if server_type == "CHUNK":
    #remove chunk files
    chunkpath = '/home/groupd/chunk/'
    for chunkFile in os.listdir(chunkpath):
        os.remove(chunkpath + chunkFile)

#go to directory
os.chdir('/home/groupd/mushroomfs')





