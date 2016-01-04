
import os
import subprocess
from setuptools import setup, Command
from glob import glob

class build_docker(Command):
    user_options = []

    def initialize_options(self):
        print "Build Docker"
    
    def run(self):
        for d in glob("docker/*/Dockerfile"):
            subprocess.check_call("docker build -t %s %s" % (os.path.basename(os.path.dirname(d)), os.path.dirname(d)), shell=True)
    
    def finalize_options(self):
        pass
      
setup(
    cmdclass={'build_docker' : build_docker},
    name='nebula',
    version='0.1.dev0',
    scripts=["bin/nebula"],
    packages=[
        'nebula',
        'nebula.docstore',
        'nebula.galaxy'
    ],
    install_requires=['requests', 'galaxy-lib'],
    license='Apache',
    long_description=open('README.md').read(),
    package_data={
        'nebula.ext.galaxy.exceptions': ['*'], 
    }
)
