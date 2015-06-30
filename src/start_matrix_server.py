from matrix_server.server import run
from debug import set_config


if __name__ == '__main__':
    set_config('/home/anauser/log/circus/matrix_server.log')
    run()
