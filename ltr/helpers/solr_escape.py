def esc_kw(kw):
    """ Take a keyword and escape all the
        Solr parts we want to escape!"""
    kw = kw.replace('\\', '\\\\') # be sure to do this first, as we inject \!
    kw = kw.replace('(', '\(')
    kw = kw.replace(')', '\)')
    kw = kw.replace('+', '\+')
    kw = kw.replace('-', '\-')
    kw = kw.replace(':', '\:')
    kw = kw.replace('/', '\/')
    kw = kw.replace(']', '\]')
    kw = kw.replace('[', '\[')
    kw = kw.replace('*', '\*')
    kw = kw.replace('?', '\?')
    kw = kw.replace('{', '\{')
    kw = kw.replace('}', '\}')
    kw = kw.replace('~', '\~')


    return kw
