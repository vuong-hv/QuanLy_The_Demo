import os

from app import app, read_bool_env
from storage import DB_FILE, init_database


def main():
    host = os.environ.get('APP_HOST', '0.0.0.0')
    port = int(os.environ.get('APP_PORT', '5001'))
    debug = read_bool_env('APP_DEBUG', default=False)
    public_ip = os.environ.get('PUBLIC_IP', '').strip()

    init_database()

    print(f'Database file: {DB_FILE}')
    if public_ip:
        print(f'Mo tren iPhone/Safari: http://{public_ip}:{port}')
    else:
        print(f'Mo tren iPhone/Safari: http://<IP-PUBLIC-CUA-SERVER>:{port}')
        print('Dat bien moi truong PUBLIC_IP de in ra dung duong dan truy cap.')

    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    main()
