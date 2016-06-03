#! /usr/bin/env python

from distutils.core import setup

setup(
    name = "londiste",
    license = "ISC",
    version = '3.3',
    maintainer = "Marko Kreen",
    maintainer_email = "markokr@gmail.com",
    py_modules = ['londiste', 'londiste.handlers'],
    install_requires = ['pgq', 'skytools', 'psycopg2'],
    scripts = ['londiste.py']
)

