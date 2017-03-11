'''Usage:

python monthly_visits.py <project_label>

This is sort of a contrived example meant to display different features.
'''
from scitran_client import ScitranClient, query, Projects
import sys
from collections import Counter
from fnmatch import fnmatch

client = ScitranClient()

# Search for the project via label
project = client.search(query(Projects).filter(Projects.label.match(sys.argv[1])))[0]

# fetch the sessions related to this project
sessions = client.request('projects/{}/sessions'.format(project['_id'])).json()

# count session by month by taking first 7 characters of date string:
# example: 2016-01-01T00:00:00, so first 7 are 2016-01
ct = Counter(
  s['timestamp'][:7]
  for s in sessions
)

# logging the months and visit counts
print 'month   | number of visits'
for month, count in sorted(ct.items(), reverse=True):
    print month, '|', count


# Let's find an image in our project to download
acquisition, f = next(
    (a, f)
    for s in sessions
    for a in client.request('sessions/{}/acquisitions'.format(s['_id'])).json()
    for f in a['files']
    if fnmatch(f['name'], '*.png')
)

client.download_file('acquisitions', acquisition['_id'], f['name'], f['hash'], dest_dir='.')
