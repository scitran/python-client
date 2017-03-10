from scitran_client import ScitranClient
import json
import random


def request(client, *args, **kwargs):
    response = client._request(*args, **kwargs)
    return json.loads(response.text)


def find(items, **kwargs):
    result = next((
        item for item in items
        if all(item[k] == v for k, v in kwargs.iteritems())
    ), None)
    return result


def _clean_session(s):
    # job is different between instances.
    # permissions site property is different between instances.
    return dict(s, job=None, permissions=None)


def _clean_acquisition(s):
    # permissions site property is different between instances.
    return dict(s, permissions=None)


def sorted_zip(a, b, key_name):
    if len(a) != len(b):
        print 'number of items do not match. left has {}, right has {}'.format(
            len(a), len(b))
    a_by_key = {item[key_name]: item for item in a}
    b_by_key = {item[key_name]: item for item in b}
    all_keys = sorted(set(a_by_key.keys() + b_by_key.keys()))
    for key in all_keys:
        a_item = a_by_key.get(key, None)
        b_item = b_by_key.get(key, None)
        assert a_item or b_item
        if not a_item:
            print key, 'missing from left'
            continue
        if not b_item:
            print key, 'missing from right'
            continue
        yield a_item, b_item


st = ScitranClient()
cni = ScitranClient('cni')


engage_st = find(request(st, 'projects'), label='ENGAGE')
engage_cni = find(request(cni, 'projects'), label='ENGAGE')

sessions_st = request(st, 'projects/{}/sessions'.format(engage_st['_id']))
sessions_cni = request(cni, 'projects/{}/sessions'.format(engage_cni['_id']))

if __name__ == '__main__':
    all_files = []

    for s_st, s_cni in sorted_zip(sessions_st, sessions_cni, '_id'):
        if _clean_session(s_st) != _clean_session(s_cni):
            print s_st['_id'], 'sessions different'
        acq_st = request(st, 'sessions/{}/acquisitions'.format(s_st['_id']))
        acq_cni = request(cni, 'sessions/{}/acquisitions'.format(s_cni['_id']))
        for a_st, a_cni in sorted_zip(acq_st, acq_cni, '_id'):
            if _clean_acquisition(a_st) != _clean_acquisition(a_cni):
                print 'file different, sess {} acq {}'.format(
                    s_st['_id'],
                    a_st['_id'],
                )
            for f_st, f_cni in sorted_zip(a_st['files'], a_cni['files'], 'name'):
                if f_st != f_cni:
                    print 'file different, sess {} acq {} file {}'.format(
                        s_st['_id'],
                        a_st['_id'],
                        f_st['name'],
                    )
                all_files.append((s_st, a_st, f_st))

        # XXX for testing
        break

    print 'checked', len(all_files), 'files'

    for s, a, f in random.sample(all_files, 3):
        cni.download_file('acquisitions', a['_id'], f['name'], f['hash'])
        # if it doesn't match, we'll throw here.
        print 'file download matches, sess {} acq {} file {}'.format(
            s['_id'],
            a['_id'],
            f['name'],
        )
