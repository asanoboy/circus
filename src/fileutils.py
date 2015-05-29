import os, fnmatch

def save_content(path, file_content):
    dir_path = os.path.dirname(path)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    mode = 'w' if type(file_content) == str else 'wb'
    f = open(path, mode)
    f.write(file_content)
    f.close()
    print('save:', path)

def find(pattern, dirname=os.getcwd()):
    matches = []
    for root, dirnames, filenames in os.walk(dirname):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches

class Workspace:
    def __init__(self, dirpath=os.getcwd()):
        self.dirpath = os.path.abspath(dirpath)
        if not os.path.isdir(self.dirpath):
            raise Exception('Path is not directory: %s' % (dirpath,))
        self.currentdir = None
        self.workdir = None

    def __enter__(self):
        self.currenddir = os.getcwd()
        index = 0
        path = None
        while 1:
            path = os.path.join(self.dirpath, 'wk%s' % (str(index),))
            if not os.path.exists(path):
                break
            index += 1
        self.workdir = path

        os.mkdir(self.workdir)
        os.chdir(self.workdir)
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        os.chdir(self.currentdir)
        os.rmdir(self.workdir)
