#!/usr/bin/env python

## file: varsplit.py
## desc: Splits the variant file produced by varget into several separate
##       files. A single file is usually too big for varload to process at once.
##       GW DB.
## vers: 0.1.0
## auth: TR
#

from sys import argv

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import util

## Script info
EXE = 'varsplit'
VERSION = '0.1.0'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

def read_variant_file(fp):
    """
    """

    ## Use a delimiter that will never be present so nothing is parsed and we 
    ## just get strings back
    return util.flatten(util.parse_generic_file(fp, delim='$!$'))

def write_lines(fp, lines):
    """
    """

    with open(fp, 'w') as fl:
        print >> fl, '## %s v. %s' % (EXE, VERSION)
        print >> fl, '## %s' % FILETAG
        print >> fl, '## last updated %s' % util.get_today()
        print >> fl, '## RSID ALLELES MA MAF CLINSIG ASSEMBLY|CHR|POS|ORIENT'
        print >> fl, '#'
        for ln in lines:
            print >> fl, ln

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <variant-file> <N>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '--verbose', action='store_true', dest='verbose', 
        help='Clutter your screen with output'
    )

    (opts, args) = parse.parse_args(argv)
    log = Log(on=opts.verbose)

    if len(args) < 3:
        log.error('[!] You need to provide a variant file')
        log.error('[!] You need to provide a split number')
        log.error('')
        parse.print_help()
        exit()

    log.info('[+] Reading variant file...')

    lines = read_variant_file(args[1])
    chunks = []
    bounds = int(len(lines) / float(int(args[2])))

    for i in range(int(args[2])):
        start = i * bounds
        end = (i + 1) * bounds

        if i == (int(args[2]) - 1):
            chunks.append(lines[start:])
        else:
            chunks.append(lines[start:end])

    log.info('[+] Writing variant files...')

    for i in range(len(chunks)):
        new_fp = '%s_%s' % (i, args[1])

        write_lines(new_fp, chunks[i])

    log.info('[+] Done!')

