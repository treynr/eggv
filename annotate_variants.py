#!/usr/bin/env python

## file: annotate_variants.py
## desc: Parses the data file produced by retrieve_variants_full.py and
##       annotates genes to variants. Intergenic variants are not mapped to
##       up/downstream genes but are saved in a separate file.
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
EXE = 'annotate_variants'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <variants> <output-file>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-a', '--append', action='store_true', dest='append', 
        help="don't write a new output file, append to an existing one"
    )
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

    if len(args) < 2:
        log.error('[!] You need to provide a processed variant file')
        log.error('[!] You need to provide an output file')
        log.error('')
        parse.print_help()
        exit()

    log.info('[+] Annotating variants...')

    ## Columns in the variant file we want:
    ## (0)  (5)                      (6)
    ## rsid geneid|sym|allele|effect assembly|chrom|position|orientation
    with open(args[1], 'r') as ifl:
        with open(args[2], 'w') as ofl:

            print >> ofl, '## %s v. %s' % (EXE, VERSION)
            print >> ofl, '## %s' % FILETAG
            print >> ofl, '## last updated %s' % util.get_today()
            print >> ofl, '## RSID ENTREZ SYMBOL ALLELE EFFECT'
            print >> ofl, '#'

            with open('intergenic-snps.tsv', 'w') as tfl:

                print >> tfl, '## %s v. %s' % (EXE, VERSION)
                print >> tfl, '## %s' % FILETAG
                print >> tfl, '## last updated %s' % util.get_today()
                print >> tfl, '## RSID CHROMOSOME POSITION ORIENTATION'
                print >> tfl, '#'

                for ln in ifl:
                    ln = ln.strip()

                    if not ln:
                        continue

                    ## Skip comments
                    if ln[0] == '#':
                        continue

                    ln = ln.split('\t')
                    rsid = ln[0]
                    genes = ln[5]
                    assembly = ln[6]

                    if genes == '.':
                        genes = None

                    else:
                        ## Isolate individual Entrez IDs and symbols, remove
                        ## duplicates
                        genes = genes.split('||')
                        genes = map(lambda g: g.split('|'), genes)
                        genes = map(lambda g: (g[0], g[1], g[2], g[3]), genes)
                        genes = list(set(genes))

                    ## If there aren't any gene annotations, this is most
                    ## likely an intergenic SNP
                    if not genes:
                        chrom = None
                        position = None
                        orientation = None

                        ## Parse out genomic coordinates so we can save that
                        ## data in the intergenic SNP file
                        for ass in assembly.split('||'):
                            ass = ass.split('|')
                            chrom = ass[1]
                            pos = ass[2]
                            orient = ass[3]

                            if chrom != '.' and chrom != '?' and\
                               pos != '.' and pos != '?':
                                   break

                            else:
                                chrom = None
                                pos = None

                        ## Sometimes there is no coordinate data
                        if not chrom or not pos:
                            continue

                        print >> tfl, '%s\t%s\t%s\t%s' % (
                            rsid, chrom, pos, orient
                        )

                    else:
                        for entrez, symbol, allele, effect in genes:
                            print >> ofl, '%s\t%s\t%s\t%s\t%s' % (
                                rsid, entrez, symbol, allele, effect
                            )

    log.info('[+] Done!')

