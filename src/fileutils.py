import os

def save_content(path, file_content):
    dir_path = os.path.dirname(path)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    mode = 'w' if type(file_content) == str else 'wb'
    f = open(path, mode)
    f.write(file_content)
    f.close()
    print('save:', path)

