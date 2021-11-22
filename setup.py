import io
import kocher_tools
from setuptools import setup

with io.open("README.rst", "rt", encoding="utf8") as f:
    readme = f.read()

# List of required non-standard python libraries
requirements = ['pyyaml',
                'pandas',
                'Biopython',
                'sqlalchemy',
                'gffutils',
                'openpyxl',
                'tox']

# Executable scripts in the package
tool_scripts = ['kocher_tools/barcode_pipeline.py',
                'kocher_tools/barcode_filter.py',
                'kocher_tools/calc_fst.py',
                'kocher_tools/calc_MK.py',
                'kocher_tools/calc_pbs.py',
                'kocher_tools/calc_pca.py',
                'kocher_tools/demultiplex_paired_barcodes.py',
                'kocher_tools/demultiplex_pipeline.py',
                'kocher_tools/create_database.py',
                'kocher_tools/insert_file.py',
                'kocher_tools/gff_position_stats.py',
                'kocher_tools/gff_chrom_stats.py',
                'kocher_tools/gff_add_features.py']

setup(name=kocher_tools.__title__,
      version=kocher_tools.__version__,
      project_urls={"Documentation": "https://kocher-guides.readthedocs.io/",
                    "Code": "https://github.com/kocherlab/kocher_tools",
                    "Issue tracker": "https://github.com/kocherlab/kocher_tools/issues"},
      license=kocher_tools.__license__,
      url=kocher_tools.__url__,
      author=kocher_tools.__author__,
      author_email=kocher_tools.__email__,
      maintainer="Andrew Webb",
      maintainer_email="19213578+aewebb80@users.noreply.github.com",
      description=kocher_tools.__summary__,
      long_description=readme,
      packages=['kocher_tools'],
      package_data={'kocher_tools': ['data/*.txt']},
      install_requires=requirements,
      scripts=tool_scripts,
      python_requires=">=3.6")
