from setuptools import setup, find_packages

setup(
    name="snowflake-generator",
    version="0.1",
    packages=find_packages(),
    install_requires=["click", "colorama", "PyYAML", "setuptools", "inquirer"],
    entry_points={
        "console_scripts": [
            "snowgen=snowgen.cli:cli",
        ],
    },
)
