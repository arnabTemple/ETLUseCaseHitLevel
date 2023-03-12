from setuptools import setup, find_packages

setup(
    name='ETLUseCaseHitLevel',
    version='1.0.0',
    url='git@github.com:arnabTemple/ETLUseCaseHitLevel.git',
    author='Arnab Dey',
    author_email='arnab.dey@temple.edu',
    description='Hit Level Use Case for ACS Interview',
    packages=find_packages(),
    install_requires=['boto3 >= 1.26.89']
)
