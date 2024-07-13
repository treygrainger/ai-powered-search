import requests
from os import path
from tqdm import tqdm

def download_one(uri, dest='data/', force=False, fancy=False):
    import os

    if not os.path.exists(dest):
        os.makedirs(dest)

    if not os.path.isdir(dest):
        raise ValueError("dest {} is not a directory".format(dest))

    filename = uri[uri.rfind('/') + 1:]
    filepath = os.path.join(dest, filename)
    if path.exists(filepath):
        if not force:
            print(filepath + ' already exists')
            return
        print("exists but force=True, Downloading anyway")

    if not fancy:
        with open(filepath, 'wb') as out:
            print('GET {}'.format(uri))
            resp = requests.get(uri, stream=True)
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
    else:
        resp = requests.get(uri, stream=True)
        total = int(resp.headers.get('content-length', 0))
        with open(filepath, 'wb') as file, tqdm(
                desc=filepath,
                total=total,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)

def extract_tgz(fname, dest='data/'):
    import tarfile
    with tarfile.open(fname, 'r:gz') as tar:
        tar.extractall(path=dest)


def download(uris, dest='data/', force=False, fancy=False):
    for uri in uris:
        download_one(uri=uri, dest=dest, force=force, fancy=fancy)
