from setuptools import setup
import os

requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
with open(requirements_path, 'r') as f:
    install_requires = [line.rstrip() for line in f]

dist = setup(
    name='Scitran Client',
    version='0.0.2',
    description='Official Scitran Client',
    author='vistalab',
    install_requires=install_requires,
    packages=['scitran_client'],
    package_data={'': ['auth.json.example']},
    platforms=['CPython 2.7'],
)
