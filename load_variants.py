#!/usr/bin/env python

## file: load_variants.py
## desc: Loads variants into the GW DB
## vers: 0.1.12
## auth: TR
#

from collections import defaultdict as dd
from functools import partial
from multiprocessing import Pool, Process
from sys import argv, maxint
import gzip
import StringIO
import subprocess
import urllib2 as url2

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import util
from gwlib import db

## Script info
EXE = 'load_variants'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <genome-build> <variants>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-a', '--allele', action='store_true', dest='allele', 
        help='create a separate variant entity for each unique allele'
    )
    parse.add_option(
        '-w', '--write', action='store_true', dest='write', 
        help='writes variant data to a file for use with the COPY command'
    )
    parse.add_option(
        '--no-info', action='store_true', dest='noinfo', 
        help="don't load variant_info data into the DB, write it to a file"
    )
    parse.add_option(
        '-d', '--dry-run', action='store_true', dest='dryrun',
        help="perform a loading dry run and don't commit any changes"
    )
    parse.add_option(
        '--verbose', action='store_true', dest='verbose', 
        help='Clutter your screen with output'
    )

    (opts, args) = parse.parse_args(argv)
    log = Log(on=opts.verbose)

    if len(args) < 3:
        log.error('[!] You need to provide a genome build')
        log.error('[!] You need to provide a variant file')
        log.error('')
        parse.print_help()
        exit()

    build_id = db.get_genome_builds_by_ref(args[1])

    if not build_id:
        log.error('[!] The build you provided is not supported')
        log.error('[!] Supported builds:')

        builds = db.get_genome_builds()

        for build in builds:
            ref_id = build['gb_ref_id']
            alt_ref_id = build['gb_alt_ref_id']

            log.error('   %s/%s' % (ref_id, alt_ref_id))

        exit()

    else:
        build_id = build_id[0]['gb_id']

    log.info('[+] Inserting variants...')

    #variants = parse_variant_file(args[2], build[0]['gb_id'])
    variants = []
    locations = []
    types = db.get_variant_type_ids_by_effect()
    unknown_type = types['unknown']
    #genome_build = db.get_genome_builds_by_ref('mm10')

    if opts.write:
        wfl = open(args[1] + '-variant-copy.tsv', 'w')

    #with open(fp, 'r') as fl:
    with open(args[2], 'r') as fl:
        print >> wfl, '\t'.join([
            'rsid',
            'allele',
            'vt_id',
            'current',
            'obs_alleles',
            'ma',
            'maf',
            'clinsig',
            'chromosome',
            'position',
            'gb_id'
        ])

        for ln in fl:
            ln = ln.strip()

            if not ln:
                continue
            if ln[0] == '#':
                continue

            ln = ln.split('\t')
            rsid = ln[0]
            observed = ln[1]
            ma = ln[2]
            maf = ln[3]
            assembly = None
            full_data = False

            ## "Lite" data
            if len(ln) == 6:
                assembly = ln[5].split('||')

            ## "Full" data
            elif len(ln) == 7:
                assembly = ln[6].split('||')
                full_data = True

            if not assembly:
                continue

            clinsig = ln[4].split('=')

            if len(clinsig) == 2:
                clinsig = clinsig[1]
            else:
                clinsig = clinsig[0]

            if clinsig == '.':
                clinsig = 'unknown'

            if ma == '.' or maf == '.':
                ma = None
                maf = None

            ## Pick the assembly that isn't missing anything
            for ass in assembly:
                ass = ass.strip().split('|')
                build = ass[0]
                chrom = ass[1]
                coord = ass[2]
                orient = ass[3]

                if build and chrom and (coord != '?') and (orient != '?'):
                    break

            ## Missing genomic coordinates
            if coord == '?' or orient == '?':
                continue

            ## The full data set associates observed alleles with their
            ## specific structural consequences, so use this instead if it's
            ## available
            if full_data:    
                ## It could be missing though
                if ln[5] == '.':
                    alleles = map(
                        lambda l: (l[0], unknown_type), observed.split('/')
                    )

                else:
                    alleles = ln[5].split('||')
                    alleles = map(lambda l: l.split('|'), alleles)
                    ## [allele, effect]
                    alleles = map(lambda l: (l[2], l[3]), alleles)
                    ## Remove any duplicate alleles, if something is overwritten
                    ## too bad
                    alleles = dict(alleles).items()

            else:
                alleles = map(
                    lambda l: (l[0], unknown_type), observed.split('/')
                )

            ## Create a variant entry for each allele
            #for allele in observed.split('/'):
            if opts.allele:
                for allele, effect in alleles:
                    variants.append({
                        'rsid': rsid.strip('rs'),
                        'allele': allele,
                        'observed': observed,
                        'ma': ma,
                        'maf': maf,
                        'clinsig': clinsig,
                        'build': build,
                        'chrom': chrom,
                        'coord': int(coord),
                        'orient': orient,
                        'effect': types[effect] if effect in types else unknown_type
                    })
            else:
                effect = alleles[0][1]
                variants.append({
                    'rsid': rsid.strip('rs'),
                    'allele': None,
                    'observed': observed,
                    'ma': ma,
                    'maf': maf,
                    'clinsig': clinsig,
                    'build': build,
                    'chrom': chrom,
                    'coord': int(coord),
                    'orient': orient,
                    'effect': types[effect] if effect in types else unknown_type
                })

            ## Genomic coordinates
            locations.append({
                'chromosome': chrom,
                'position': coord, 
                'gb_id': build_id
            })

            ## Only works with allele so I don't have to write more code
            if opts.noinfo:

                for v in variants:
                    print >> wfl, '\t'.join([
                        v['rsid'],
                        v['allele'] if v['allele'] else 'NULL',
                        str(v['effect']),
                        't',
                        v['observed'], 
                        v['ma'] if v['ma'] else 'NULL',
                        v['maf'] if v['maf'] else 'NULL',
                        v['clinsig'],
                        locations[0]['chromosome'],
                        locations[0]['position'],
                        str(locations[0]['gb_id'])
                    ])

                variants = []
                locations = []
            else:
                if len(locations) > 5000:

                    vris = db.insert_variant_infos(locations)
                    vri_map = dd(lambda: dd(int))

                    for vri in vris:
                        chrom = vri['vri_chromosome']
                        pos = vri['vri_position']

                        vri_map[chrom][pos] = vri['vri_id']

                    for v in variants:
                        v['vri_id'] = vri_map[v['chrom']][v['coord']]

                        if opts.write:
                            print >> wfl, '\t'.join([
                                v['rsid'],
                                v['allele'],
                                str(v['effect']),
                                't',
                                v['observed'], 
                                v['ma'] if v['ma'] else 'NULL',
                                v['maf'] if v['maf'] else 'NULL',
                                v['clinsig'],
                                str(v['vri_id'])
                            ])

                    if not opts.write:
                        db.insert_variants(variants)

                    variants = []
                    locations = []

    if opts.write:
        wfl.close()

    log.info('[+] Committing changes...')

    if not opts.dryrun:
        db.commit()

    log.info('[+] Done!')
