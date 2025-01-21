import os
from distutils.core import setup

from setuptools import find_packages

setup(
    name='ebi_eva_common_pyutils',
    scripts=[os.path.join(os.path.dirname(__file__), 'ebi_eva_internal_pyutils', 'archive_directory.py')],
    packages=find_packages(),
    version='0.6.12',
    license='Apache',
    description='EBI EVA - Common Python Utilities',
    url='https://github.com/EBIVariation/eva-common-pyutils',
    keywords=['EBI', 'EVA', 'PYTHON', 'UTILITIES'],
    install_requires=['requests', 'lxml', 'pyyaml', 'cached-property', 'retry'],
    extras_require={'eva-internal': ['psycopg2-binary', 'pymongo', 'networkx<=2.5']},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ]
)
