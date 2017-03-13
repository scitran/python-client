from scitran_client import query, Projects, Sessions, Groups


def test_query():
    assert query(Sessions).filter(
        Projects.label.match('ADHD cuing task'),
        Groups.label.match('ADHDLab'),
    ) == dict(
        path='sessions',
        projects=dict(filtered=dict(filter={'and': [
            dict(query=dict(match=dict(label='ADHD cuing task'))),
        ]})),
        groups=dict(filtered=dict(filter={'and': [
            dict(query=dict(match=dict(label='ADHDLab'))),
        ]})),
    )
