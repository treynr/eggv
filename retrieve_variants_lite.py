#!/usr/bin/env python

## file: retrieve_variants_lite.py
## desc: Retrieves variant data from NCBI for a particular species and stores
##       it in an intermediate format that can be used for insertion into the 
##       GW DB. This format does not include gene annotations or functional
##       consequences but is faster and uses less disk space.
## vers: 0.1.12
## auth: TR
#

from functools import partial
from multiprocessing import Pool, Process
from sys import argv
import os
import subprocess

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import util

## Script info
EXE = 'retrieve_variants_lite'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

## Mouse variant repository
MM10_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/mouse_10090/ASN1_flat/'
## Human variant repos
HG37_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b149_GRCh37p13/ASN1_flat/'
HG38_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/ASN1_flat/'
## Mouse chromosomes
MOUSE_CHROMS = range(1, 20) + ['X', 'Y']
## Human chromosomes
HUMAN_CHROMS = range(1, 23) + ['X', 'Y']

TEMPORARY_SNP_FILE_GZ = 'temp_snp.txt.gz'
TEMPORARY_SNP_FILE = 'temp_snp.txt'

def make_flat_file_name(chromosome):
    """
    Generates the filename for an ASN1 flat file that can be found under NCBI's
    dbSNP FTP. Each filename is exactly the same except for the chromosome
    number.
    """

    return 'ds_flat_ch%s.flat.gz' % chromosome
    #return 'ds_ch%s.xml.gz' % chromosome

def download_snp_file(ftp_url, chromosome):
    """
    Downloads and gunzips an ASN1 SNP flat file using the given species FTP and
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

    #os.remove(gzfp)

    #try:
    #    ## This is a ton of data, sometimes > 1GB
    #    snp_file = url2.urlopen(url)
    #    snpgz = snp_file.read()

    #except:
    #    return None

    #snpgz = StringIO.StringIO(snpgz)
    #snpfl = gzip.GzipFile(mode='r', fileobj=snpgz)

    #contents = snpfl.read()
    #snpfl.close()

    return fp

def process_snps(ifp, ofp):
    """
    Simultaneously parses NCBI's dbSNP ASN1 files and writes the parsed SNP
    data out to a file.
    SNPs are written as they are parsed to minimize memory use.

    arguments
        ifp:    input filepath
        ofp:    output filepath

    """

    #ofp = 'chr' + str(chrom) + '-' + ofp
    snps = []

    with open(ifp, 'r') as ifl:
        with open(ofp, 'a') as ofl:
            ## Default values for fields we care about
            snp = {
                'rsid': None,
                'alleles': None,
                'assemblies': [],
                'clinsig': 'unknown',
                'maf_allele': None,
                'maf': None,
            }

            for ln in ifl:
                cols = ln.strip().split(' | ')

                ## RefSNP line
                if cols[0][:2] == 'rs':

                    ## Reset SNP data
                    if snp['rsid']:
                        #snps.append(snp)
                        write_snp(ofl, snp)

                        snp  = {}
                        snp = {
                            'rsid': None,
                            'alleles': None,
                            'assemblies': [],
                            'clinsig': 'unknown',
                            'maf_allele': None,
                            'maf': None,
                        }

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
                

    #return snps

def write_snp(fl, snp):
    """
    Write variant data to a file.
    Append variant data to a file.
    """

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
    usage = '%s [options] <genome-build> <output-file>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-a', '--append', action='store_true', dest='append', 
        help="don't write a new output file, append to an existing one"
    )
    parse.add_option(
        '-p', '--procs', action='store', dest='procs', type='int', default=1,
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

    if opts.procs > 1:
        log.warn('[!] Be careful using the -p/--procs options')
        log.warn('[!] Using >1 process will use significant amounts of disk space')
        log.warn('[!] Downloading and parsing chromosomes 1-4 uses ~500GB')

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

        ## Arguments for SNP processing function: ASN filepath, output filepath
        pargs = zip(dl_files, ['chr' + str(c) + '-' + args[2] for c in chroms])

        ## Parse out SNPs, write them to a file in parallel
        procs = [pool.apply_async(process_snps, p) for p in pargs]

        ## Wait for everything to finish--kind of inefficient
        for p in procs:
            p.wait()

        ## Remove ASN files to free up disk space
        for dl in dl_files:
            os.remove(dl)

    log.info('[+] Done!')

