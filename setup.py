import io
import kocher_tools
from setuptools import setup

with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

requirements = ['pyyaml',
                'pandas',
                'Biopython',
                'networkx']

tool_scripts = ['kocher_tools/barcode_pipeline.py',
                'kocher_tools/barcode_filter.py',
                'kocher_tools/create_database.py',
                'kocher_tools/retrieve_samples.py',
                'kocher_tools/upload_samples.py']

setup(name=kocher_tools.__title__,
      version=kocher_tools.__version__,
      project_urls={"Documentation": "https://ppp.readthedocs.io",
                    "Code": "https://github.com/jaredgk/PPP",
                    "Issue tracker": "https://github.com/jaredgk/PPP/issues"},
      license=kocher_tools.__license__,
      url=kocher_tools.__url__,
      author=kocher_tools.__author__,
      author_email=kocher_tools.__email__,
      maintainer="Andrew Webb",
      maintainer_email="19213578+aewebb80@users.noreply.github.com",
      description=kocher_tools.__summary__,
      long_description=readme,
      include_package_data=True,
      packages=['kocher_tools'],
      install_requires=requirements,
      scripts=tool_scripts,
      python_requires=">=3.7")