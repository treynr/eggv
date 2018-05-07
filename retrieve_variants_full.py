#!/usr/bin/env python

## file: retrieve_variants_full.py
## desc: Retrieves variant data from NCBI for a particular species and stores
##       it in an intermediate format that can be used for insertion into the 
##       GW DB. This format includes gene annotations and functional
##       consequences.
## vers: 0.1.12
## auth: TR
#

from functools import partial
from multiprocessing import Pool, Process
from sys import argv
import os
import subprocess
from lxml import etree as et

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import util

## Script info
EXE = 'retrieve_variants_full'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

## Mouse variant repository
#MM10_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/mouse_10090/XML/'
MM10_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/archive/mouse_10090/XML/'
## Human variant repos
HG37_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b149_GRCh37p13/XML/'
HG38_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/XML/'
## Mouse chromosomes
MOUSE_CHROMS = range(1, 20) + ['X', 'Y']
## Human chromosomes
HUMAN_CHROMS = range(1, 23) + ['X', 'Y']

TEMPORARY_SNP_FILE_GZ = 'temp_snp.xml.gz'
TEMPORARY_SNP_FILE = 'temp_snp.xml'

def make_flat_file_name(chromosome):
    """
    Generates the filename for an ASN1 flat file that can be found under NCBI's
    dbSNP FTP. Each filename is exactly the same except for the chromosome
    number.
    """

    return 'ds_ch%s.xml.gz' % chromosome

def download_snp_file(ftp_url, chromosome):
    """
    Downloads and gunzips a SNP XML file using the given species FTP and
    chromosome.
    """

    url = ftp_url + make_flat_file_name(chromosome)

    gzfp = 'chr' + str(chromosome) + '-' + TEMPORARY_SNP_FILE_GZ
    fp = 'chr' + str(chromosome) + '-' + TEMPORARY_SNP_FILE

    subprocess.check_output([
        'wget', 
        '--quiet', 
        url,
        '-O',
        gzfp
    ])

    subprocess.check_output([
        'gunzip', 
        '-q', 
        '-f', 
        gzfp
    ])

    return fp


def remove_namespace(elem, ns):
    """
    Removes namespaces from XML element tags. The XML NCBI uses causes every
    the string 'https://www.ncbi.nlm.nih.gov/SNP/docsum' to be prepended to
    every element tag. This function removes that stupid string.

    arguments
        elem:   XML element object
        ns:     namespace string to remove

    returns
        the XML element without the namespace string attached to the tag
    """

    ns = '{%s}' % ns

    if elem.tag.startswith(ns):
        elem.tag = elem.tag[len(ns):]
    
    return elem

def process_snps(ifp, ofp):
    """
    Simultaneously parses NCBI's dbSNP XML files and writes the parsed SNP
    data out to a file.
    SNPs are written as they are parsed to minimize memory use.

    arguments
        ifp:    input filepath
        ofp:    output filepath

    """

    snps = []
    ncbi_namespace = ''

    with open(ofp, 'w') as ofl:
        print >> ofl, '## %s v. %s' % (EXE, VERSION)
        print >> ofl, '## %s' % FILETAG
        print >> ofl, '## last updated %s' % util.get_today()
        print >> ofl, '## RSID ALLELES MA MAF CLINSIG GENEID|SYMBOL|ALLELE|EFFECT ASSEMBLY|CHR|POS|ORIENT'
        print >> ofl, '#'

        ## Opens the input SNP file as an iterator to lessen memory usage
        for event, elem in et.iterparse(ifp, events=('end', 'start-ns')):

            ## Collect namespaces so we can remove the NCBI NS from element
            ## tags
            if event == 'start-ns':
                prefix, ns = elem

                if prefix == '':
                    ncbi_namespace = ns

            if event == 'end':
                elem = remove_namespace(elem, ncbi_namespace)

                ## Only look at SNP elements
                if elem.tag != 'Rs':
                    continue

                rsid = elem.attrib.get('rsId')

                ## This should be impossible...
                if not rsid:
                    continue

                ## Minor allele frequency + minor allele
                frequency = elem.find('Frequency')

                if frequency is None:
                    ma = '.'
                    maf = '.'

                else:
                    ma = frequency.attrib.get('allele', '.')
                    maf = frequency.attrib.get('freq', '.')

                ## Only human SNPs have clinical significance data attached
                clinsig = elem.find('Phenotype/ClinicalSignificance')

                if clinsig is None:
                    clinsig = '.'
                else:
                    clinsig = clinsig.text

                ## Genome assembly and genomic coordinates
                assembly = elem.find('Assembly')

                ## Ignore SNPs lacking genome build and assembly metadata
                if assembly is None:
                    continue

                ## <Sequence> <Observed> contains observed alleles
                observed = elem.find('Sequence/Observed')

                if observed is None:
                    observed = '.'
                else:
                    observed = observed.text

                ## Genome build in the form e.g. GRCh38.p7
                build = assembly.attrib.get('groupLabel', '.')

                ## <Assembly> <Component> contains chromosome location and 
                ## other random shit
                chromosome = assembly.find('Component')

                ## Can't do anything without a chromosome
                if chromosome is None:
                    continue
                else:
                    chromosome = chromosome.attrib.get('chromosome')

                    if not chromosome:
                        continue

                ## <Assembly> <Component> <MapLoc> contains genomic
                ## coordinates.
                maploc = assembly.find('.//MapLoc')

                if maploc is None:
                    continue
                ## Ignore SNPs that don't have exact genomic coordinates
                if 'physMapInt' not in maploc.attrib:
                   continue
                ## Ignore SNPs that don't have orientations
                if 'orient' not in maploc.attrib:
                   continue

                ## For whatever fucking reason genomic coordinates are 1 - 
                ## the actual location. This only occurs in the XML output,
                ## all other SNP file formats (like ASN) have the exact 
                ## location. If you don't believe me go look up some
                ## coordinates in dbSNP and compare them to these shitty XML
                ## files.
                coordinate = 1 + int(maploc.attrib['physMapInt'])
                orient = maploc.attrib.get('orient', '')

                ## Change orientation to +/- syntax and ignore anything
                ## that doesn't have the proper data
                if orient == 'forward':
                    orient = '+'

                elif orient == 'reverse':
                    orient = '-'

                else:
                    continue

                annotations = maploc.findall('FxnSet')
                genes = []

                for ann in annotations:
                    ## Skip references cause idk what they are and they
                    ## don't seem to have sequence ontology annotations
                    if ann.attrib['fxnClass'] == 'reference':
                        continue

                    ## Ignore things without a sequence ontology term
                    if 'soTerm' not in ann.attrib:
                        continue
                    ## If there's no Entrez ID or symbol for this gene, 
                    ## it's probably not worth saving.
                    if 'geneId' not in ann.attrib:
                        continue
                    ## In some cases, there are even SNPs with a gene ID 
                    ## of 'null' (see rs3737573)
                    if 'symbol' not in ann.attrib:
                        continue

                    genes.append((
                        ann.attrib['geneId'],
                        ann.attrib['symbol'],
                        ann.attrib.get('allele', '.'),
                        ann.attrib['soTerm']
                    ))

                if not genes:
                    genes = '.'

                else:
                    genes = list(set(genes))
                    genes = map(lambda g: '|'.join(g), genes)
                    genes = '||'.join(genes)

                assembly = '%s|%s|%s|%s' % (
                    build, chromosome, coordinate, orient
                )

                print >> ofl, '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                    rsid, observed, ma, maf, clinsig, genes, assembly
                )

                ## Free this element's memory
                elem.clear()

                ## Free parent element memory
                while elem.getprevious() is not None:
                    del elem.getparent()[0]


def check_build(build):
    """
    Checks to see if the user entered genome build is supported. Returns the
    proper FTP URL if it is. 

    arguments
        build: genome build

    returns
        returns a tuple containing the FTP and organism chromosomes for the
        given build
    """

    build = build.lower()

    if build == 'mm10' or build == 'grcm38':
        return (MM10_FTP, MOUSE_CHROMS)

    if build == 'hg38' or build == 'grch38':
        return (HG38_FTP, HUMAN_CHROMS)

    ## hg37 support is disabled
    #if build == 'hg19' or build == 'hg37' or build == 'grch37':
    #    return (HG37_FTP, HUMAN_CHROMS)

    return (None, None)

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <genome-build> <output-file>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-p', '--procs', action='store', dest='procs', type='int', default=1,
        help="number of parallel processes to use for processing variants"
    )
    parse.add_option(
        '--verbose', action='store_true', dest='verbose', 
        help='Clutter your screen with output'
    )

    (opts, args) = parse.parse_args(argv)
    log = Log(on=opts.verbose)

    if len(args) < 3:
        log.error('[!] You need to provide a genome build, supported bulids:')
        log.error('\t mm10/GRCm38')
        log.error('\t hg38/GRCh38')
        log.error('\t hg19/hg37/GRCh37')
        log.error('[!] You need to provide an output file')
        log.error('')
        parse.print_help()
        exit()

    ftp_url, chromosomes = check_build(args[1])

    if not ftp_url or not chromosomes:
        log.error('[!] The genome build you entered is not supported')
        exit()

    if opts.procs > 1:
        log.warn('[!] Be careful using the -p/--procs options')
        log.warn('[!] Using >1 process will use significant amounts of disk space')
        log.warn('[!] Downloading and parsing hg38 chromosomes 1-4 uses ~400GB')

    all_snps = []

    pool = Pool(processes=(opts.procs))

    ## Breaks the list of chromosomes into sublists depending on the number
    ## of processes given
    for chroms in util.chunk_list(chromosomes, opts.procs):
        chroms = map(str, chroms)

        log.info('[+] Retrieving SNP data for chr %s...' % ', '.join(chroms))

        ## Generate a partial function that can be pickled for multiprocessing
        partial_download_snps = partial(download_snp_file, ftp_url)

        ## Download SNPs in parallel
        dls = [pool.apply_async(partial_download_snps, (c,)) for c in chroms]

        ## Wait for everything to finish--kind of inefficient
        dl_files = [dl.get() for dl in dls]

        log.info('[+] Parsing and writing SNP data...')

        ## Arguments for SNP processing function: XML filepath, output filepath
        pargs = zip(dl_files, ['chr' + str(c) + '-' + args[2] for c in chroms])

        ## Parse out SNPs, write them to a file in parallel
        procs = [pool.apply_async(process_snps, p) for p in pargs]

        ## Wait for everything to finish--kind of inefficient
        for p in procs:
            p.wait()

        ## Remove XML files to free up disk space
        for dl in dl_files:
            os.remove(dl)

    log.info('[+] Done!')

