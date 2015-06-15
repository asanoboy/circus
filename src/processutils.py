import subprocess
import sys


def command(cmd, output=True):
    print(cmd)
    if output:
        p = subprocess.Popen(
            cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        p.wait()
        return
    else:
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.wait()
        stdout_data, stderr_data = p.communicate()
        return stdout_data.decode('utf-8')
