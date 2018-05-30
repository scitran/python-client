import scitran_client.flywheel_analyzer as fa
from scitran_client import ScitranClient

client = ScitranClient('stanfordlabs')


'''
These labels should be unique and descriptive for the analysis
you are running. So, if the analyzer finds a program with the same
name as these, it will assume it was run with the same settings and
proceed, feeding it into the next step of the pipeline.

So, when you are doing multiple runs with different parameters, or
for different b-values, make sure to have unique names here.
'''
dtiinit_label = 'dtiinit1000'
afq_label = 'afq1000'


def dtiinit_inputs(acquisitions, **kwargs):
    diffusion = fa.find(acquisitions, label='Diffusion')

    return dict(
        bvec=diffusion.find_file('*1000.bvec'),
        bval=diffusion.find_file('*1000.bval'),
        dwi=diffusion.find_file('*1000.nii.gz'),
    ), dict(
        # Here you can add any configuration. So for instance to add robust tensor, do:
        # fitMethod='rt'
    )


def afq_inputs(analyses, **kwargs):
    dtiinit = fa.find(analyses, label=dtiinit_label)

    return dict(
        dtiInit_Archive=dtiinit.find_file('dtiInit_*.zip'),
    )


if __name__ == '__main__':
    with fa.installed_client(client):
        ops = [
            fa.define_analysis(
                'dtiinit', dtiinit_inputs,
                label=dtiinit_label),
            fa.define_analysis(
                'afq', afq_inputs,
                label=afq_label),
        ]
        fa.run(
            ops,
            project=fa.find_project(label='HCP_preproc'),
            session_limit=1)
