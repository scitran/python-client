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

        '''
        an = request(st, 'sessions/{}'.format(s_st['_id'])).get('analyses')
        # https://flywheel.scitran.stanford.edu/api/sessions/57a163957023e50021e2f952/analyses/58c0a684609b3800136fa569?root=true
        for analysis in an:
            num_outputs = len([f for f in analysis['files'] if f.get('output')])
            # if not analysis['job']['saved_files'] and not num_outputs:
            if not num_outputs:
                # xxx add root=true
                print '{}, saved {}, num out {} DELETE "sessions/{}/analyses/{}?root=true"'.format(
                    analysis['job']['name'],
                    len(analysis['job']['saved_files']),
                    num_outputs,
                    s_st['_id'],
                    analysis['_id'],
                )
        continue
        '''

        try:
            analyses_st = request(st, 'sessions/{}'.format(s_st['_id'])).get('analyses')
            analyses_cni = request(cni, 'sessions/{}'.format(s_cni['_id'])).get('analyses')
        except Exception as e:
            print 'eeek', s_st['_id'], str(e)
            continue
        for an_st, an_cni in sorted_zip(analyses_st, analyses_cni, '_id'):
            if an_st != an_cni:
                print 'analysis different, sess {} analysis {}'.format(
                    s_st['_id'],
                    an_st['_id'],
                )
            for f_st, f_cni in sorted_zip(an_st['files'], an_cni['files'], 'name'):
                all_files.append((s_st, an_st, f_st, 'analysis'))

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
                all_files.append((s_st, a_st, f_st, 'acquisitions'))

    print 'checked', len(all_files), 'files'

    for s, a, f, container_type in random.sample(all_files, 100):
        if container_type == 'acquisitions':
            cni.download_file(container_type, a['_id'], f['name'], f['hash'])
        else:
            cni.download_file('sessions', s['_id'], f['name'], f['hash'], analysis_id=a['_id'])
        # if it doesn't match, we'll throw here.
        print 'file download matches, sess {} {} {} file {}'.format(
            s['_id'],
            container_type,
            a['_id'],
            f['name'],
        )
