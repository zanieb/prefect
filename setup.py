import sys

from setuptools import find_packages, setup

from setuptools.command import easy_install
import re
TEMPLATE = '''\
# -*- coding: utf-8 -*-
# EASY-INSTALL-ENTRY-SCRIPT: '{3}','{4}','{5}'
__requires__ = '{3}'
import re
import sys

from {0} import {1}

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit({2}())'''


@classmethod
def get_args(cls, dist, header=None):  # noqa: D205,D400
    """
    Yield write_script() argument tuples for a distribution's
    console_scripts and gui_scripts entry points.
    """
    if header is None:
        # pylint: disable=E1101
        header = cls.get_header()
    spec = str(dist.as_requirement())
    for type_ in 'console', 'gui':
        group = type_ + '_scripts'
        for name, ep in dist.get_entry_map(group).items():
            # ensure_safe_name
            if re.search(r'[\\/]', name):
                raise ValueError("Path separators not allowed in script names")
            script_text = TEMPLATE.format(
                ep.module_name, ep.attrs[0], '.'.join(ep.attrs),
                spec, group, name)
            # pylint: disable=E1101
            args = cls._get_script_args(type_, name, header, script_text)
            for res in args:
                yield res


# pylint: disable=E1101
easy_install.ScriptWriter.get_args = get_args

import versioneer

## base requirements
install_requires = open("requirements.txt").read().strip().split("\n")
dev_requires = open("dev-requirements.txt").read().strip().split("\n")

extras = {
    "airtable": ["airtable-python-wrapper >= 0.11, < 0.12"],
    "aws": ["boto3 >= 1.9, < 2.0"],
    "azure": [
        "azure-storage-blob >= 2.1.0, < 3.0",
        "azureml-sdk >= 1.0.65, < 1.1",
        "azure-cosmos >= 3.1.1, <3.2",
    ],
    "dev": dev_requires,
    "dropbox": ["dropbox ~= 9.0"],
    "google": [
        "google-cloud-bigquery >= 1.6.0, < 2.0",
        "google-cloud-storage >= 1.13, < 2.0",
    ],
    "kubernetes": ["kubernetes >= 9.0.0a1, < 10.0", "dask-kubernetes >= 0.8.0"],
    "rss": ["feedparser >= 5.0.1, < 6.0"],
    "postgres": ["psycopg2-binary >= 2.8.2"],
    "snowflake": ["snowflake-connector-python >= 1.8.2, < 2.0"],
    "redis": ["redis >= 3.2.1"],
    "spacy": ["spacy >= 2.0.0, < 3.0.0"],
    "templates": ["jinja2 >= 2.0, < 3.0"],
    "viz": ["graphviz >= 0.8.3"],
    "twitter": ["tweepy >= 3.5, < 4.0"],
}

if sys.version_info < (3, 6):
    extras["dev"].remove("black")

extras["all_extras"] = sum(extras.values(), [])


setup(
    name="prefect",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=install_requires,
    extras_require=extras,
    scripts=[],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    entry_points={"console_scripts": ["prefect=prefect.cli:cli"]},
    python_requires=">=3.5.2",
    description="The Prefect Core automation and scheduling engine.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://www.github.com/PrefectHQ/prefect",
    license="Apache License 2.0",
    author="Prefect Technologies, Inc.",
    author_email="help@prefect.io",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Monitoring",
    ],
)
