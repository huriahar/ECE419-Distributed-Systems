import subprocess
import re
import sys

print "Running lsof -i"
p = subprocess.Popen(['lsof', '-i'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = p.communicate()

print "Finding KVServer process with port {}".format(sys.argv[1])
for proc in out.split('\n'):
    processRE = r'java\s+(\d+) \S+\s+\S+\s+IPv\d \d+\s+\dt\d\s+TCP \*\:{} \(LISTEN\)'.format(sys.argv[1])
    matchObj = re.match(processRE, proc)
    if matchObj is not None:
        pid = matchObj.group(1)
        print "Found process. Pid of process is {}".format(pid)
        p = subprocess.Popen(['kill', '-9', pid], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        assert out is ""
        assert err is ""
        print "Successfully killed process"
        break
print "Done."
