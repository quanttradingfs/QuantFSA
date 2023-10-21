from setuptools import setup, find_packages

def read_requirements(file):
    with open(file) as f:
        return f.read().splitlines()

requirements = read_requirements("requirements.txt")

setup(
    name = 'QuantFSA',
    author = 'FSA',
	version = '0.1',
    install_requires = requirements,
)