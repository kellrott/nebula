

from setuptools import setup

setup(
    name='nebula',
    version='0.1dev',
    scripts=["bin/nebula"],
    packages=[
        'nebula',
        'nebula.docstore',
        'nebula.galaxy',
        'nebula.service',
        'nebula.tasks',
        'nebula.ext',
        'nebula.ext.galaxy',
        'nebula.ext.galaxy.exceptions',
        'nebula.ext.galaxy.objectstore',
        'nebula.ext.galaxy.util'
    ],
    install_requires=['requests'],
    license='Apache',
    long_description=open('README.md').read(),
    package_data={
        'nebula.ext.galaxy.exceptions': ['*'], 
    }
)
