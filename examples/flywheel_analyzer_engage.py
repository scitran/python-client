import scitran_client.flywheel_analyzer as fa
from scitran_client import ScitranClient

client = ScitranClient('https://flywheel-cni.scitran.stanford.edu')
with fa.installed_client(client):
    project = fa.find_project(label='ENGAGE')
    sessions = client.request('projects/{}/sessions'.format(project['_id'])).json()
    session_by_subject = {}
    second_to_first_visit_id = {}
    for s in sessions:
        subject = s['subject']['code'][:7].upper()
        session_by_subject.setdefault(subject, []).append(s)
    for subject, subject_sessions in session_by_subject.iteritems():
        # we need at least two sessions
        if len(subject_sessions) < 2:
            continue
        subject_sessions.sort(key=lambda s: s['timestamp'])
        # HACK this is a bit of a heuristic. this will certainly fail
        # for some folks that skipped a BV, or folks that missed the 2mo
        # but hopefully, those folks will otherwise have data that is just
        # fine.
        second_to_first_visit_id[subject_sessions[1]['_id']] = subject_sessions[0]['_id']


# XXX at least make this be just the first thing without ' 2'?
label_to_task_type = {
    'go-no-go 2': 'gonogo',
    'conscious 2': 'conscious',
    'nonconscious 2': 'nonconscious',
    'EmoReg': 'EmoReg',
}
label_to_behavioral_pattern = {
    'go-no-go 2': '*_GoNoGo.txt',
    'conscious 2': '*_EmotionConscious.txt',
    'nonconscious 2': '*_EmotionNonconscious.txt',
}


def _find_file(container, glob):
    return (
        container.find_file(glob, default=None) or
        # HACK because flywheel does not currently support nested files
        # in output folders, we are flattening hierarchy by replacing
        # forward slashes with @@
        container.find_file(glob.replace('/', '@@')))


def analysis_label(gear_name, acquisition_label):
    return '{} ({})'.format(gear_name, acquisition_label)


def define_analysis(gear_name, acquisition_label, create_inputs):
    '''Light wrapper around define_analysis that lets us pass in an acquisition_label
    in to create_inputs.
    '''
    return fa.define_analysis(
        gear_name,
        lambda **kwargs: create_inputs(acquisition_label, **kwargs),
        label=analysis_label(gear_name, acquisition_label))


def reactivity_inputs(acquisition_label, acquisitions, session, **kwargs):
    functional = fa.find_required_input_source(acquisitions, label=acquisition_label)
    # using plain find() here b/c this T1w might be missing
    structural = fa.find(acquisitions, label='T1w 1mm')
    if not structural:
        assert session['_id'] in second_to_first_visit_id,\
            'the only sessions that should be missing T1w are second visits. {} was missing a T1w'\
            .format(session['_id'])
        first_visit_session_id = second_to_first_visit_id[session['_id']]
        first_visit_acquisitions = client.request(
            'sessions/{}/acquisitions'.format(first_visit_session_id)).json()
        structural = fa.find(first_visit_acquisitions, label='T1w 1mm')
        assert structural, 'Session {} is missing a structural.'.format(session['_id'])

    return dict(
        functional=functional.find_file('*.nii.gz'),
        structural=structural.find_file('*.nii.gz'),
    )


def connectivity_inputs(acquisition_label, analyses, **kwargs):
    reactivity = fa.find_required_input_source(
        analyses, label=analysis_label('reactivity-preprocessing', acquisition_label))

    return dict(
        functional=_find_file(reactivity, 'realigned_unwarped_files/*.nii'),
        highres2standard_warp=_find_file(reactivity, 'highres2standard_warp/*.nii.gz'),
        example_func2highres=_find_file(reactivity, 'example_func2highres_mat/*.mat'),
    )


def first_level_model_inputs(acquisition_label, analyses, acquisitions, **kwargs):
    reactivity = fa.find_required_input_source(
        analyses, label=analysis_label('reactivity-preprocessing', acquisition_label))
    connectivity = fa.find_required_input_source(
        analyses, label=analysis_label('connectivity-preprocessing', acquisition_label))
    behavioral = fa.find_required_input_source(
        acquisitions, label='Behavioral and Physiological')
    behavioral_file = behavioral.find_file(label_to_behavioral_pattern[acquisition_label], default=None)
    if not behavioral_file:
        raise fa.SkipOperation()

    return dict(
        reactivity_functional=_find_file(reactivity, 'smoothed/s02_globalremoved_func_data.nii'),
        connectivity_functional=_find_file(connectivity, 'result/swa01_normalized_func_data.nii'),
        behavioral=behavioral_file,
        structural_brain_fnirt_mask=_find_file(reactivity, 'brain_fnirt_mask/*.nii.gz'),
        example_func=_find_file(reactivity, 'example_func/*.nii.gz'),
        highres2example_func=_find_file(reactivity, 'highres2example_func_mat/*.mat'),
        example_func2highres=_find_file(reactivity, 'example_func2highres_mat/*.mat'),
        highres2standard_warp=_find_file(reactivity, 'highres2standard_warp/*.nii.gz'),
        spike_regressors_wFD=_find_file(reactivity, 'wFD/spike_regressors_wFD.mat'),
    ), dict(task_type=label_to_task_type[acquisition_label])

if __name__ == '__main__':
    with fa.installed_client(client):
        fa.run([
            define_analysis('reactivity-preprocessing', 'go-no-go 2', reactivity_inputs),
            define_analysis('connectivity-preprocessing', 'go-no-go 2', connectivity_inputs),
            define_analysis('first-level-models', 'go-no-go 2', first_level_model_inputs),

            define_analysis('reactivity-preprocessing', 'conscious 2', reactivity_inputs),
            define_analysis('connectivity-preprocessing', 'conscious 2', connectivity_inputs),
            define_analysis('first-level-models', 'conscious 2', first_level_model_inputs),

            define_analysis('reactivity-preprocessing', 'nonconscious 2', reactivity_inputs),
            define_analysis('connectivity-preprocessing', 'nonconscious 2', connectivity_inputs),
            define_analysis('first-level-models', 'nonconscious 2', first_level_model_inputs),

            define_analysis('reactivity-preprocessing', 'EmoReg', reactivity_inputs),
            define_analysis('connectivity-preprocessing', 'EmoReg', connectivity_inputs),
            # define_analysis('first-level-models', 'EmoReg', first_level_model_inputs),
        ], project=project)
