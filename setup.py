from setuptools import setup, find_packages

setup(
    name="chonker",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "chonker = chonker:main",
        ],
    },
)
