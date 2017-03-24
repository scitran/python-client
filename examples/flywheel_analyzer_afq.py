import scitran_client.flywheel_analyzer as fa
from scitran_client import ScitranClient

client = ScitranClient('https://flywheel-cni.scitran.stanford.edu')


def dtiinit_inputs(acquisitions, **kwargs):
    diffusion = fa.find(acquisitions, measurement='diffusion')

    return dict(
        bvec=diffusion.find_file('*.bvec'),
        bval=diffusion.find_file('*.bval'),
        nifti=diffusion.find_file('*.nii.gz'),
    )


def afq_inputs(analyses, **kwargs):
    dtiinit = fa.find(analyses, label='dtiinit')

    return dict(
        dtiInit_Archive=dtiinit.find_file('dtiInit_*.zip'),
    )

if __name__ == '__main__':
    with fa.installed_client(client):
        fa.run([
            fa.define_analysis('dtiinit', dtiinit_inputs, label_matcher=lambda val: val.startswith('dtiinit ')),
            fa.define_analysis('afq', afq_inputs, label_matcher=lambda val: val.startswith('afq ')),
        ], project=fa.find_project(label='ENGAGE'))
