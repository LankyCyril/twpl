import setuptools
from types import SimpleNamespace


meta = SimpleNamespace(
    __name__ = "twpl",
    __version__ = "0.1.3",
    __author__ = "Kirill Grigorev",
    __git_id__ = "LankyCyril",
    __license__ = "GPLv3",
    __description__ = "Two-phase locking via lockfiles",
)


def get_readme(filename):
    try:
        with open(filename) as readme_handle:
            return readme_handle.read()
    except FileNotFoundError:
        return meta.__description__


if __name__ == "__main__":
    setuptools.setup(
        name = meta.__name__,
        version = meta.__version__,
        packages = [meta.__name__],
        url = f"https://github.com/{meta.__git_id__}/{meta.__name__}/",
        author = meta.__author__,
        license = meta.__license__,
        zip_safe = True,
        description = "Two-phase locking via lockfiles",
        long_description = get_readme("readme.md"),
        long_description_content_type = "text/markdown",
        classifiers = [
            "Development Status :: 3 - Alpha",
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
            "Intended Audience :: Developers",
            "Topic :: Software Development",
            "Topic :: System",
        ],
        python_requires = ">=3.7",
        install_requires = ["filelock"],
    )
