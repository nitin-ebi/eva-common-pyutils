from distutils.core import setup

from setuptools import find_packages

setup(
    name='ebi_eva_common_pyutils',
    packages=find_packages(),
    version='0.2.8',
    license='Apache',
    description='EBI EVA - Common Python Utilities',
    url='https://github.com/EBIVariation/eva-common-pyutils',
    keywords=['EBI', 'EVA', 'PYTHON', 'UTILITIES'],
    install_requires=['psycopg2', 'requests', 'pymongo', 'lxml'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ]
)
