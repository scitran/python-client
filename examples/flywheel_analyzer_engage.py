import scitran_client.flywheel_analyzer as fa


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
        container.find_file(glob) or
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


def reactivity_inputs(acquisition_label, acquisitions, **kwargs):
    functional = fa.find(acquisitions, label=acquisition_label)
    structural = fa.find(acquisitions, label='T1w 1mm')

    return dict(
        functional=functional.find_file('*.nii.gz'),
        structural=structural.find_file('*.nii.gz'),
    )


def connectivity_inputs(acquisition_label, analyses, acquisitions):
    reactivity = fa.find(
        analyses, label=analysis_label('reactivity-preprocessing', acquisition_label))

    return dict(
        functional=_find_file(reactivity, 'realigned_unwarped_files/*.nii'),
        highres2standard_warp=_find_file(reactivity, 'highres2standard_warp/*.nii.gz'),
        example_func2highres=_find_file(reactivity, 'example_func2highres_mat/*.mat'),
    )


def first_level_model_inputs(acquisition_label, analyses, acquisitions):
    reactivity = fa.find(
        analyses, label=analysis_label('reactivity-preprocessing', acquisition_label))
    connectivity = fa.find(
        analyses, label=analysis_label('connectivity-preprocessing', acquisition_label))
    behavioral = fa.find(
        acquisitions, label='Behavioral and Physiological')

    return dict(
        reactivity_functional=_find_file(reactivity, 'smoothed/s02_globalremoved_func_data.nii'),
        connectivity_functional=_find_file(connectivity, 'result/swa01_normalized_func_data.nii'),
        behavioral=behavioral.find_file(label_to_behavioral_pattern[acquisition_label]),
        structural_brain_fnirt_mask=_find_file(reactivity, 'brain_fnirt_mask/*.nii.gz'),
        example_func=_find_file(reactivity, 'example_func/*.nii.gz'),
        highres2example_func=_find_file(reactivity, 'highres2example_func_mat/*.mat'),
        example_func2highres=_find_file(reactivity, 'example_func2highres_mat/*.mat'),
        highres2standard_warp=_find_file(reactivity, 'highres2standard_warp/*.nii.gz'),
        spike_regressors_wFD=_find_file(reactivity, 'wFD/spike_regressors_wFD.mat'),
    ), dict(task_type=label_to_task_type[acquisition_label])

if __name__ == '__main__':
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
    ], project=fa.find_project(label='ENGAGE'), session_limit=1)
