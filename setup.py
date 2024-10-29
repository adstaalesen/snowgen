from setuptools import setup, find_packages

setup(
    name="snowgen",
    version="0.1.0",
    description="A tool for managing Snowflake databases and schemas",
    author="Adrian S. Staalesen",
    url="https://github.com/adstaalesen/snowgen",
    packages=find_packages(include=["snowgen", "snowgen.*"]),
    install_requires=[
        "click",
        "colorama",
        "PyYAML",
        "setuptools",
        "inquirer",
        "pathlib",
    ],
    entry_points={
        "console_scripts": [
            "snowgen=snowgen.cli:cli",  # Adjust this to your CLI entry point
        ],
    },
)
