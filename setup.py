import os
import sys
from setuptools import setup, find_packages

command = sys.argv[-1]
if command == 'publish':
    os.system('rm -rf dist')
    os.system('python3 setup.py sdist')
    os.system('python3 setup.py bdist_wheel')
    os.system('twine upload dist/*whl dist/*gz')
    sys.exit()

with open("README.md", "rt") as fh:
    long_description = fh.read()

install_requires = [
    "configargparse>=1.4.1",
#    "hail",
    "mock",
#    "coverage",
]

setup(
    name='step_pipeline',
    version="0.1",
    description="Pipeline library that simplifies creation of pipelines that run on top of hail Batch and other compute enviornments",
    install_requires=install_requires,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(where="src/"),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires=">=3.7",
    license="MIT",
    keywords='pipelines, workflows, hail, Batch, cromwell, Terra, dsub, SGE',
    url='https://github.com/broadinstitute/step_pipeline',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
