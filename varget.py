#!/usr/bin/env python

## file: varget.py
## desc: Retrieves variant data from NCBI for a particular species and stores
##       it in an intermediate format that can be used for insertion into the 
##       GW DB.
## vers: 0.1.0
## auth: TR
#

import gzip
import StringIO
import urllib2 as url2
from sys import argv

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import ncbi
from gwlib import util

## Script info
EXE = 'varget'
VERSION = '0.1.0'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

## Mouse variant repository
MM10_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/mouse_10090/ASN1_flat/'
## Human variant repos
HG37_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b149_GRCh37p13/ASN1_flat/'
HG38_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b149_GRCh38p7/ASN1_flat/'
## Mouse chromosomes
MOUSE_CHROMS = range(1, 20) + ['X', 'Y']
## Human chromosomes
HUMAN_CHROMS = range(1, 23) + ['X', 'Y']

def make_flat_file_name(chromosome):
    """
    Generates the filename for an ASN1 flat file that can be found under NCBI's
    dbSNP FTP. Each filename is exactly the same except for the chromosome
    number.
    """

    return 'ds_flat_ch%s.flat.gz' % chromosome

def download_snp_file(ftp_url, chromosome):
    """
    Downloads and gunzips an ASN1 SNP flat file using the given species FTP and
    chromosome.
    """

    url = ftp_url + make_flat_file_name(chromosome)

    try:
        ## This is a ton of data, sometimes > 1GB
        snp_file = url2.urlopen(url)
        snpgz = snp_file.read()

    except:
        return None

    snpgz = StringIO.StringIO(snpgz)
    snpfl = gzip.GzipFile(mode='r', fileobj=snpgz)

    contents = snpfl.read()
    snpfl.close()

    return contents

def parse_asn1_flat_file(fl):
    """
    Parses NCBI's propietary ASN1 flat file format for dbSNP data.
    A description of the format can be found here:
        ftp://ftp.ncbi.nih.gov/snp/00readme.txt

    """

    snps = []

    ## Each SNP data chunk is separated by newlines
    fl = fl.split('\n\n')

    for chunk in fl:
        chunks = chunk.split('\n')
        ## Default values for fields we care about
        snp = {
            'rsid': None,
            'alleles': None,
            'assemblies': [],
            'clinsig': 'unknown',
            'maf_allele': None,
            'maf': None,
        }

        for ln in chunks:
            cols = ln.split(' | ')

            ## RefSNP line
            if cols[0][:2] == 'rs':

                snp['rsid'] = cols[0]
                
            ## SNP allele line
            elif cols[0][:3] == 'SNP':

                alleles = cols[1].split('=')
                alleles = alleles[1].strip("'")

                snp['alleles'] = alleles

            ## Contig/assembly info line
            elif cols[0][:3] == 'CTG':

                ass = cols[1].split('=')
                chromosome = cols[2].split('=')
                position = cols[3].split('=')
                orient = cols[8].split('=')
                ## Removes the patch (e.g. p7) portion of the assembly
                ass = ass[1].split('.')[0]
                chromosome = chromosome[1]
                position = position[1]
                orient = orient[1]

                assembly = {}
                assembly['assembly'] = ass
                assembly['chromosome'] = chromosome
                assembly['position'] = position
                assembly['orient'] = orient

                snp['assemblies'].append(assembly)

            ## Clinical Significance line
            elif len(cols[0]) >= 7 and cols[0][:7] == 'CLINSIG':

                snp['clinsig'] = cols[1]

            ## Minor allele frequency line
            elif len(cols[0]) >= 4 and cols[0][:4] == 'GMAF':

                allele = cols[1].split('=')
                maf = cols[3].split('=')
                allele = allele[1]
                maf = maf[1]

                snp['maf_allele'] = allele
                snp['maf'] = maf
                
        ## Probably the flat file header
        if not snp['rsid']:
            continue

        snps.append(snp)

    return snps

def write_snps(fp, snps):
    """
    Write variant data to a file.
    Append variant data to a file.
    """

    with open(fp, 'a') as fl:

        for snp in snps:
            assemblies = []

            for ass in snp['assemblies']:
                assemblies.append('%s|%s|%s|%s' % (
                    ass['assembly'], 
                    ass['chromosome'], 
                    ass['position'], 
                    ass['orient']
                ))

            outstr = '%s\t%s\t%s\t%s\t%s\t%s' % (
                snp['rsid'],
                snp['alleles'],
                snp['maf_allele'] if snp['maf_allele'] else '.',
                snp['maf'] if snp['maf'] else '.',
                snp['clinsig'],
                '||'.join(assemblies)
            )

            print >> fl, outstr

def touch_file(fp):
    """
    Creates the initial output file and header.
    """

    with open(fp, 'w') as fl:
        print >> fl, '## %s v. %s' % (EXE, VERSION)
        print >> fl, '## %s' % FILETAG
        print >> fl, '## last updated %s' % util.get_today()
        print >> fl, '## RSID ALLELES MAF_ALLELE MAF CLINSIG ASSEMBLY|CHR|POS|ORIENT'
        print >> fl, '#'

def check_build(build):
    """
    Checks to see if the user entered genome build is supported. Returns the
    proper FTP URL if it is. 
    """

    build = build.lower()

    if build == 'mm10' or build == 'grcm38':
        return (MM10_FTP, MOUSE_CHROMS)

    if build == 'hg38' or build == 'grch38':
        return (HG38_FTP, HUMAN_CHROMS)

    if build == 'hg19' or build == 'hg37' or build == 'grch37':
        return (HG37_FTP, HUMAN_CHROMS)

    return (None, None)

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <genome-build> <output>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-a', '--append', action='store_true', dest='append', 
        help="don't write a new output file, append to an existing one"
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
        log.error('\t hg19/GRCh37')
        log.error('[!] You need to provide an output file')
        log.error('')
        parse.print_help()
        exit()

    ftp_url, chromosomes = check_build(args[1])

    if not ftp_url or not chromosomes:
        log.error('[!] The genome build you entered is not supported')
        exit()

    all_snps = []
    
    if not opts.append:
        touch_file(args[2])

    ## This method: retrieving the ASN1 file, unzipping, and parsing everything
    ## in memory uses a ton of RAM. This worked fine on a server with 64GB but
    ## will probably shit the bed on servers with less memory.
    for chrom in chromosomes:
        log.info('[+] Retrieving SNP data for chr%s...' % chrom)

        contents = download_snp_file(ftp_url, chrom)

        if not contents:
            log.warn('[!] No chr%s data exists, skipping...' % chrom)
            continue

        log.info('[+] Parsing SNP data...')

        snps = parse_asn1_flat_file(contents)

        if not contents:
            log.warn('[!] No chr%s SNPs were parsed, skipping...' % chrom)
            continue

        log.info('[+] Writing SNP data...')

        ## Periodically write things to a file otherwise the memory usage never
        ## stops growing
        write_snps(args[2], snps)

        del contents
        del snps

    log.info('[+] Done!')

