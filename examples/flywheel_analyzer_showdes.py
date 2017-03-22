from scitran_client import ScitranClient
import scitran_client.flywheel_analyzer as fa

client = ScitranClient('https://flywheel.scitran.stanford.edu')

with fa.installed_client(client):
    D99 = fa.find(
        client.request('sessions/588bd1ac449f9800159305c2/acquisitions').json(),
        label='atlas')

    # generate a label for these "Logothetis DES" sessions that matches the
    # "showdes" sessions.
    functional_acquisitions_by_session_label = {
        s['label'].replace('.', '').lower():
        client.request('sessions/{}/acquisitions'.format(s['_id'])).json()
        for s in client.request('projects/584b0330bf6dd80015881071/sessions').json()
    }


def anatomical_warp_inputs(acquisitions, **kwargs):
    anatomical = fa.find(acquisitions, label='anatomical_mdeft')
    return dict(
        native=anatomical.find_file('*.nii.gz'),
        standard=D99.find_file('D99_template.nii.gz'),
        warp_target=D99.find_file('D99_atlas_1.2a.nii.gz'),
    )


def functional_warp_inputs(idx, acquisitions, session, analyses, **kwargs):
    functional_acquisitions = functional_acquisitions_by_session_label[session['label']]
    label = 'functional_{}'.format(idx)
    func = fa.find(functional_acquisitions, label=label)
    if not func:
        raise fa.SkipOperation('Could not find functional {}'.format(label))

    anat_warp = fa.find(analyses, label='afni-brain-warp')
    base_anat = '{}_0001_mdeft'.format(session['label'])

    return dict(
        anatomical=anat_warp.find_file('{}.nii'.format(base_anat)),
        warp_target=anat_warp.find_file('{}_seg.nii'.format(base_anat)),
        functional=func.find_file('*.nii.gz'),
    )

if __name__ == '__main__':
    with fa.installed_client(client):
        fa.run([
            fa.define_analysis('afni-brain-warp', anatomical_warp_inputs, label='anatomical warp'),
        ] + [
            fa.define_analysis(
                'afni-brain-coreg',
                lambda **kwargs: functional_warp_inputs(idx, **kwargs),
                label='afni-brain-coreg-{}'.format(idx))
            for idx in range(100)
        ], project=fa.find_project(label='showdes'), max_workers=2)
