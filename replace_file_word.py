import fnmatch
import os

with open('token', 'r') as token_file:
    token = token_file.read()

token = token.strip()
token = token[2:token.__len__() - 2]


def find_replace(directory, find, replace, filePattern):
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in fnmatch.filter(files, filePattern):
            filepath = os.path.join(path, filename)
            with open(filepath) as f:
                s = f.read()
            s = s.replace(find, replace)
            with open(filepath, "w") as f:
                f.write(s)


find_replace("./", "#vault_token#", token, "*.json")
