"""Setup for Londiste.
"""

from setuptools import setup

CLI_NAME = 'londiste'

setup(
    name = "londiste",
    description = "Database replication based on PgQ",
    url = "https://github.com/pgq/londiste",
    license = "ISC",
    version = '3.4',
    maintainer = "Marko Kreen",
    maintainer_email = "markokr@gmail.com",
    packages = ['londiste', 'londiste.handlers'],
    install_requires = ['pgq', 'skytools', 'psycopg2'],
    entry_points = {
        'console_scripts': [
            CLI_NAME + ' = londiste.cli:main',
        ],
    },
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: ISC License (ISCL)",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: System :: Clustering",
        "Topic :: Utilities",
    ]
)

