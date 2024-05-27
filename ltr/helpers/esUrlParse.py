def parseUrl(fullEsUrl):
    from urllib.parse import urlsplit, urlunsplit
    import os.path
    o = urlsplit(fullEsUrl)

    esUrl = urlunsplit([o.scheme, o.netloc, '','',''])

    indexAndSearchType = os.path.split(o.path)

    return (esUrl, indexAndSearchType[0][1:], indexAndSearchType[1])


if __name__ == "__main__":
    from sys import argv
    print(parseUrl(argv[1]))
