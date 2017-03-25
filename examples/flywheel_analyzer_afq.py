import scitran_client.flywheel_analyzer as fa
from scitran_client import ScitranClient
import re

client = ScitranClient('https://flywheel-cni.scitran.stanford.edu')


def prefix_matcher(prefix):
    # doing this funny prefix matching to catch both "afq" and "afq 2017-01-01..."
    return lambda val: val == prefix or val.startswith(prefix + ' ')

dtiinit_matcher = prefix_matcher('dtiinit')
afq_matcher = prefix_matcher('afq')


def dtiinit_inputs(acquisitions, **kwargs):
    diffusion = fa.find(acquisitions, label='DTI 2mm b1250 84dir(axial)')

    return dict(
        bvec=diffusion.find_file('*.bvec'),
        bval=diffusion.find_file('*.bval'),
        nifti=diffusion.find_file('*.nii.gz'),
    )


def afq_inputs(analyses, **kwargs):
    dtiinit = fa.find(analyses, label=dtiinit_matcher)

    return dict(
        dtiInit_Archive=dtiinit.find_file('dtiInit_*.zip'),
    )

if __name__ == '__main__':
    with fa.installed_client(client):
        fa.run([
            fa.define_analysis(
                'dtiinit', dtiinit_inputs,
                label_matcher=dtiinit_matcher),
            fa.define_analysis(
                'afq', afq_inputs,
                label_matcher=afq_matcher),
        ], project=fa.find_project(label='ENGAGE'))
