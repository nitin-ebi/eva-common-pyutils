import os
from distutils.core import setup

from setuptools import find_packages

setup(
    name='ebi_eva_common_pyutils',
    scripts=[os.path.join(os.path.dirname(__file__), 'ebi_eva_internal_pyutils', 'archive_directory.py')],
    packages=find_packages(),
    version='0.7.1',
    license='Apache',
    description='EBI EVA - Common Python Utilities',
    url='https://github.com/EBIVariation/eva-common-pyutils',
    keywords=['EBI', 'EVA', 'PYTHON', 'UTILITIES'],
    install_requires=['requests==2.*', 'lxml>4.9,==4.*', 'pyyaml==6.*', 'cached-property==1.5.*', 'retry>0.9,==0.*',
                      'openpyxl==3.*'],
    extras_require={'eva-internal': ['psycopg2-binary', 'pymongo', 'networkx<=2.5']},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ]
)
