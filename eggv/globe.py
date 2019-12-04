#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: globe.py
## desc: Contains global variables, mostly output directories and files used by
##       the pipeline scripts.

from pathlib import Path
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())


class Globals(object):
    """
    Singleton Globals class for representing the species specific global variables,
    mostly directories and filepaths.
    """

    _instance = None
    build = None

    def __new__(cls, *args, **kwargs):

        if cls._instance is None:
            cls._instance = super(Globals, cls).__new__(cls)

        return cls._instance

    def __init__(self, datadir: str = 'data/', build: str = 'hg38'):

        if self.build is None:
            self.base_dir = Path(datadir)
            self.build = build

            self.format_paths()
            self.create_data_directories()

            ## URLs

            ## Ensembl variation build v. 95
            ## For humans, Ensembl stores variants separately per chromosome, so this URL must
            ## be substituted w/ the chromosome
            self.url_hg38_variation = 'http://ftp.ensembl.org/pub/release-95/variation/gvf/homo_sapiens/homo_sapiens_incl_consequences-chr{}.gvf.gz'
            self.url_mm10_variation = 'http://ftp.ensembl.org/pub/release-95/variation/gvf/mus_musculus/mus_musculus_incl_consequences.gvf.gz'

            ## Ensembl gene build v. 95
            self.url_hg38_gene = 'http://ftp.ensembl.org/pub/release-95/gtf/homo_sapiens/Homo_sapiens.GRCh38.95.gtf.gz'
            self.url_mm10_gene = 'http://ftp.ensembl.org/pub/release-95/gtf/mus_musculus/Mus_musculus.GRCm38.95.gtf.gz'

            ## Variables
            self.var_human_chromosomes = [str(c) for c in range(1, 23)] + ['X', 'Y']
            self.var_mouse_chromosomes = [str(c) for c in range(1, 20)] + ['X', 'Y']

    def format_paths(self):
        """
        Format directories and filepaths.
        """

        ## Generate filepaths using the given base data directory and build
        self.dir_data = Path(self.base_dir)

        ## Variant directories
        self.dir_variant = Path(self.dir_data, 'variants', self.build)
        self.dir_variant_raw = Path(self.dir_variant, 'raw')
        self.dir_variant_effects = Path(self.dir_variant, 'effects')
        self.dir_variant_meta = Path(self.dir_variant, 'meta')

        ## Gene directories
        self.dir_gene = Path(self.dir_data, 'genes', self.build)
        self.dir_gene_raw = Path(self.dir_gene, 'raw')
        self.dir_gene_meta = Path(self.dir_gene, 'meta')

        ## Annotation directories
        self.dir_annotated = Path(self.dir_variant, 'annotated')
        self.dir_annotated_inter = Path(self.dir_annotated, 'intergenic')
        self.dir_annotated_intra = Path(self.dir_annotated, 'intragenic')

        ## Output filepaths
        ## Genes
        self.fp_gene_compressed = Path(
            self.dir_gene_raw, f'{self.build}-gene-build.gtf.gz'
        )
        self.fp_gene_raw = Path(self.dir_gene_raw, f'{self.build}-gene-build.gtf')
        self.fp_gene_meta = Path(self.dir_gene_meta, f'{self.build}-gene-build.tsv')
        self.fp_gene_dedup = Path(
            self.dir_gene_meta, f'{self.build}-gene-build-dedup.tsv'
        )

        ## Variants (most of this is only for mouse since human variants are split
        ## up by chromosome
        self.fp_variant_compressed = Path(
            self.dir_variant_raw, f'{self.build}-variant-build.gvf.gz'
        )
        self.fp_variant_raw = Path(
            self.dir_variant_raw, f'{self.build}-variant-build.gvf'
        )
        self.fp_variant_effects = Path(
            self.dir_variant_effects, f'{self.build}-variant-effects.tsv'
        )
        self.fp_variant_meta = Path(
            self.dir_variant_meta, f'{self.build}-variant-metadata.tsv'
        )
        self.fp_annotated_inter = Path(
            self.dir_annotated_inter, f'{self.build}-intergenic-variants.tsv'
        )
        self.fp_annotated_intra = Path(
            self.dir_annotated_intra, f'{self.build}-intragenic-variants.tsv'
        )

        self.create_data_directories()

    def create_data_directories(self):
        """
        Generate the data directories.
        """

        try:
            self.dir_variant_raw.mkdir(exist_ok=True, parents=True)
            self.dir_variant_effects.mkdir(exist_ok=True, parents=True)
            self.dir_variant_meta.mkdir(exist_ok=True, parents=True)

            self.dir_gene_raw.mkdir(exist_ok=True, parents=True)
            self.dir_gene_meta.mkdir(exist_ok=True, parents=True)

            self.dir_annotated_inter.mkdir(exist_ok=True, parents=True)
            self.dir_annotated_intra.mkdir(exist_ok=True, parents=True)

        except OSError as e:
            logging.getLogger(__name__).error('Could not make data directories: %s', e)
            exit(1)

    def reinitialize(self, base_dir=None, build=None):
        """
        Reformat and recreate filepaths using new base directory and/or build.
        """

        reinit = False

        if base_dir and self.base_dir != base_dir:
            self.base_dir = base_dir
            reinit = True

        if build and self.build != build:
            self.build = build
            reinit = True

        if reinit:
            self.format_paths()
            self.create_data_directories()

        return self._instance
