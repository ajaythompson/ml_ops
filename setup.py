#!/usr/bin/env python

"""The setup script."""

from typing import Dict, List

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'pyspark>=2.4.5',
    'Click',
    'networkx',
    'jsonschema',
    'pyyaml',
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

EXTRAS_REQUIREMENTS: Dict[str, List[str]] = {}

sklearn = [
    'scikit-learn',
    'pandas'
]

EXTRAS_REQUIREMENTS['sklearn'] = sklearn

ui = [
    'flask==2.0.1'
]

EXTRAS_REQUIREMENTS['ui'] = ui

_all_requirements = list(
    {req for extras_reqs in EXTRAS_REQUIREMENTS.values() for req in
     extras_reqs})

EXTRAS_REQUIREMENTS['all'] = _all_requirements

setup(
    author="Ajay Thompson",
    author_email='ajaythompson@gmail.com',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    description="Framework to operationalize machine learning.",
    entry_points={
        'console_scripts': [
            'ml_ops=ml_ops.cli:main',
            'run-workflow=ml_ops.cli:run_workflow'
        ],
    },
    install_requires=requirements,
    extras_require=EXTRAS_REQUIREMENTS,
    license="GNU General Public License v3",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='ml_ops',
    name='ml_ops',
    packages=find_packages(include=['ml_ops', 'ml_ops.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/ajaythompson/ml_ops',
    version='0.1.0',
    zip_safe=False,
)
