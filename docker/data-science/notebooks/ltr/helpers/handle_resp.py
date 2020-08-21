

def resp_msg(msg, resp, throw=True):
    print('{} [Status: {}]'.format(msg, resp.status_code))
    if resp.status_code >= 400:
        print(resp.text)
        if throw:
            raise RuntimeError(resp.text)

