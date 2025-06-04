import os

def get_scripts_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

def script_cmd(script_name):
    return f'python3 {os.path.join(get_scripts_dir(), script_name)}'
