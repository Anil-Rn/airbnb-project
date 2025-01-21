from setuptools import find_packages, setup

setup(
    name="airbnb",
    packages=find_packages(exclude=["airbnb_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
