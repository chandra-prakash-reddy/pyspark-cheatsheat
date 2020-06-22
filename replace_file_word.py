import fnmatch
import os
import argparse


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


parser = argparse.ArgumentParser(description='replace vault token')
parser.add_argument('--t', help='provide token_file_name')
parser.add_argument('--d', help='directory_path of settings json')
args = parser.parse_args()
update_token_in_settings(str(args.t),str(args.d))
