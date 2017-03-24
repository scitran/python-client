import scitran_client.flywheel_analyzer as fa
from scitran_client import ScitranClient
import re

client = ScitranClient('https://flywheel-cni.scitran.stanford.edu')


def dtiinit_inputs(acquisitions, **kwargs):
    diffusion = fa.find(acquisitions, label='DTI 2mm b1250 84dir(axial)')

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
        # doing this funny prefix matching to catch both "afq" and "afq 2017-01-01..."
        fa.run([
            fa.define_analysis(
                'dtiinit', dtiinit_inputs,
                label_matcher=lambda val: re.match(r'dtiinit($| )', val)),
            fa.define_analysis(
                'afq', afq_inputs,
                label_matcher=lambda val: re.match(r'afq($| )', val)),
        ], project=fa.find_project(label='ENGAGE'))
