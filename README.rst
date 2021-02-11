Kocher-Tools
============

.. image:: https://www.travis-ci.com/kocherlab/kocher_tools.svg?branch=master
    :target: https://www.travis-ci.com/kocherlab/kocher_tools

Installation
------------

.. code-block:: bash

    git clone https://github.com/kocherlab/kocher_tools
    cd kocher_tools/
    python setup.py install

If you are running anaconda and don’t have blast, vsearch, or fastq-multx, they may be installed using:

.. code-block:: bash

    conda install -c bioconda blast
    conda install -c bioconda fastq-multx
    conda install -c bioconda vsearch
