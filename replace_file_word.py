import fnmatch
import os
from os import environ


def find_replace(directory, find, replace, filePattern):
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in fnmatch.filter(files, filePattern):
            filepath = os.path.join(path, filename)
            with open(filepath) as f:
                s = f.read()
            s = s.replace(find, replace)
            with open(filepath, "w") as f:
                f.write(s)


def update_token_in_settings(token_path,directory_path):
    print('token_file : {} directory_path : {}'.format(token_path, directory_path))
    with open(token_path, 'r') as token_file:
        token = token_file.read()

    token = token.strip()
    token = token[2:token.__len__() - 2]
    find_replace(directory_path, "#vault_token#", token, "*.json")



update_token_in_settings(str(environ.get('token_file')),str(environ.get('directory_path')))
