from setuptools import setup, find_packages
import pathlib
import re

this_dir = pathlib.Path(__file__).parent.absolute()
project_name = "DE_project"
package_dir = "scripts"
path_to_init_file = this_dir / package_dir / project_name / "__init__.py"

with open(this_dir / "README.md", encoding="utf-8") as file:
    long_description = file.read()

# Read the contents of requirements.txt
with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

def get_property(property: str, path_to_init_file: pathlib.Path) -> str:
    """
    Reads a property from the project's __init__.py
    e.g. get_property("__version__") --> "1.2.3"
    """
    regex = re.compile(r"{}\s*=\s*[\"'](?P<value>[^\"']*)[\"']".format(property))
    try:
        with open(path_to_init_file) as initfh:
            try:
                result = regex.search(initfh.read()).group("value")
            except AttributeError:
                result = None
    except FileNotFoundError:
        result = None
    return result

setup(
    name=project_name,
    version=get_property("version", path_to_init_file.parent / "version.py"),
    description="Data Engineering Data Science",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=get_property("__author__", path_to_init_file),
    author_email=get_property("__author_email__", path_to_init_file),
    url=get_property("__repository__", path_to_init_file),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    keywords="data-engineering data-science",
    package_dir={"": package_dir},
    packages=find_packages(where=package_dir),
    package_data={
        project_name: ["data/*.csv"],  # Example: Include all CSV files in a data directory
    },
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [f"{project_name} = {project_name}.__main__:main"],
    },
    platforms=["any"],
)
