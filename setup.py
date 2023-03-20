import os
from setuptools import setup, find_packages

readme = os.path.join(os.path.dirname(__file__), "README.rst")

DESCRIPTION = "Python SQLAlchemy Dialect for JDBCAPI."

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name="sqlalchemy_jdbcapi_timestream",
    version="1.0.0",
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author="Mai Nguyen",
    author_email="janetvn@gmail.com",
    license="Apache",
    url="https://github.com/janetvn/sqlalchemy_timestream",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(include=["sqlalchemy_jdbcapi_timestream"]),
    include_package_data=True,
    install_requires=["SQLAlchemy", "JayDeBeApi"],
    zip_safe=False,
    keywords="SQLAlchemy JDBCAPI Dialect for Amazon Timestream using JDBC connection",
    entry_points={
        "sqlalchemy.dialects": [
            "jdbcapi.timestream = sqlalchemy_timestream.timestreamjdbc:TimestreamJDBCDialect",
        ]
    },
)