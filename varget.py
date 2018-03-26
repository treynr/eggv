#!/usr/bin/env python

## file: varget.py
## desc: Retrieves variant data from NCBI for a particular species and stores
##       it in an intermediate format that can be used for insertion into the 
##       GW DB.
## vers: 0.1.12
## auth: TR
#

from functools import partial
from multiprocessing import Pool, Process
from sys import argv
import gzip
import os
import StringIO
import urllib2 as url2
import subprocess
import xml.etree.ElementTree as et

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import util

## Script info
EXE = 'varget'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

## Mouse variant repository
MM10_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/mouse_10090/ASN1_flat/'
## Human variant repos
HG37_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b149_GRCh37p13/ASN1_flat/'
HG38_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/ASN1_flat/'
HG38_FTP = 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/XML/'
## Mouse chromosomes
MOUSE_CHROMS = range(1, 20) + ['X', 'Y']
## Human chromosomes
HUMAN_CHROMS = range(1, 23) + ['X', 'Y']

TEMPORARY_SNP_FILE_GZ = 'temp_snp.txt.gz'
TEMPORARY_SNP_FILE = 'temp_snp.txt'
TEMPORARY_SNP_FILE_GZ = 'temp_snp.xml.gz'
TEMPORARY_SNP_FILE = 'temp_snp.xml'

def make_flat_file_name(chromosome):
    """
    Generates the filename for an ASN1 flat file that can be found under NCBI's
    dbSNP FTP. Each filename is exactly the same except for the chromosome
    number.
    """

    #return 'ds_flat_ch%s.flat.gz' % chromosome
    return 'ds_ch%s.xml.gz' % chromosome

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

def delete_temp_files():
    """
    """

    #os.remove(TEMPORARY_SNP_FILE_GZ)
    os.remove(TEMPORARY_SNP_FILE)

def process_snps(ifp, ofp):
    """
    Parses NCBI's propietary ASN1 flat file format for dbSNP data.
    A description of the format can be found here:
        ftp://ftp.ncbi.nih.gov/snp/00readme.txt

    """

    #ofp = 'chr' + str(chrom) + '-' + ofp
    snps = []

    ## Each SNP data chunk is separated by newlines
    #fl = fl.split('\n\n')

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
                #ln = chunk.split('\n')

                #for ln in chunks:
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

def remove_namespace(elem, ns):
    """
    """

    ns = '{%s}' % ns

    if elem.tag.startswith(ns):
        elem.tag = elem.tag[len(ns):]
    
    return elem

def process_snps_xml(ifp, ofp):
    """
    Parses NCBI's dbSNP XML files.

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
        for event, elem in et.iterparse(ifp, events=('start', 'end', 'start-ns')):

            ## Collect namespaces so we can remove the NCBI NS from element
            ## tags
            if event == 'start-ns':
                prefix, ns = elem

                if prefix == '':
                    ncbi_namespace = ns

            if event == 'end':
                elem = remove_namespace(elem, ncbi_namespace)

                ## This is a SNP element
                if elem.tag == 'Rs':
                    rsid = elem.attrib['rsId']

                    frequency = elem.find('Frequency')

                    if frequency is not None:
                        ma = frequency.attrib['allele']
                        maf = frequency.attrib['freq']

                    else:
                        ma = '.'
                        maf = '.'

                    clinsig = elem.find('Phenotype/ClinicalSignificance')

                    if clinsig is None:
                        clinsig = '.'
                    else:
                        clinsig = clinsig.text

                    ## Genome assembly and genomic coordinates
                    assembly = elem.find('Assembly')
                    ## <Sequence> <Observed> contains observed alleles
                    observed = elem.find('Sequence/Observed').text

                    ## Ignore SNPs lacking genome build and assembly metadata
                    if assembly is None:
                        continue

                    ## Genome build in the form e.g. GRCh38.p7
                    build = assembly.attrib['groupLabel']

                    ## <Assembly> <Component> contains chromosome location and 
                    ## other random shit
                    chromosome = assembly.find('Component').attrib['chromosome']

                    ## <Assembly> <Component> <MapLoc> contains genomic
                    ## coordinates.
                    maploc = assembly.find('.//MapLoc')

                    ## Ignore SNPs that don't have exact genomic coordinates or
                    ## orientations
                    if 'physMapInt' not in maploc.attrib or\
                       'orient' not in maploc.attrib:
                           continue

                    ## For whatever fucking reason genomic coordinates are 1 - 
                    ## the actual location. This only occurs in the XML output,
                    ## all other SNP file formats (like ASN) have the exact 
                    ## location. If you don't believe me go look up some
                    ## coordinates in dbSNP.
                    coordinate = 1 + int(maploc.attrib['physMapInt'])
                    orient = maploc.attrib['orient']

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

                        genes.append((
                            ann.attrib['geneId'],
                            ann.attrib['symbol'],
                            ann.attrib['allele'] if 'allele' in ann.attrib else '.',
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


def touch_file(fp):
    """
    Creates the initial output file and header.
    """

    with open(fp, 'w') as fl:
        print >> fl, '## %s v. %s' % (EXE, VERSION)
        print >> fl, '## %s' % FILETAG
        print >> fl, '## last updated %s' % util.get_today()
        print >> fl, '## RSID ALLELES MA MAF CLINSIG ASSEMBLY|CHR|POS|ORIENT'
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
        log.warn('[!] Downloading and parsing chromosomes 1-4 uses ~250GB')

    all_snps = []
    
    #if not opts.append:
    #    touch_file(args[2])

    pool = Pool(processes=(opts.procs))

    ## This method: retrieving the ASN1 file, unzipping, and parsing everything
    ## in memory uses a ton of RAM. This worked fine on a server with 64GB but
    ## will probably shit the bed on servers with less memory.
    #for chrom in chromosomes:
    for chroms in util.chunk_list(chromosomes, opts.procs):
        #chroms = map(str, chroms)

        #log.info('[+] Retrieving SNP data for chr %s...' % ', '.join(chroms))
        log.info('[+] Retrieving SNP data...')


        partial_download_snps = partial(download_snp_file, ftp_url)
        #partial_download_snps(chroms[0])
        #contents = download_snp_file(ftp_url, chrom)
        dls = [pool.apply_async(partial_download_snps, (c,)) for c in chroms]
        dl_files = [dl.get() for dl in dls]

        #if not contents:
        #    log.warn('[!] No chr%s data exists, skipping...' % chrom)
        #    continue

        log.info('[+] Parsing and writing SNP data...')

        pargs = zip(dl_files, ['chr' + str(c) + '-' + args[2] for c in chroms])
        #snps = parse_asn1_flat_file(TEMPORARY_SNP_FILE)
        #process_snps(TEMPORARY_SNP_FILE, args[2])
        #procs = [pool.apply_async(process_snps, p) for p in pargs]
        procs = [pool.apply_async(process_snps_xml, p) for p in pargs]

        for p in procs:
            p.wait()

        for dl in dl_files:
            os.remove(dl)

        #if not snps:
        #    log.warn('[!] No chr%s SNPs were parsed, skipping...' % chrom)
        #    continue

        #log.info('[+] Writing SNP data...')

        ## Periodically write things to a file otherwise the memory usage never
        ## stops growing
        #write_snps(args[2], snps)

        #del snps

    log.info('[+] Done!')

