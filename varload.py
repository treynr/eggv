#!/usr/bin/env python

## file: varload.py
## desc: Uses the variant data file produced by varget to insert variant
##       information into the GW DB. The variant schema additions must be
##       incorporated prior to running this script.
## vers: 0.1.12
## auth: TR
#

import os
import StringIO
import time
import urllib2 as url2
from sys import argv

## my libs, which can be retreived with the command:
## git clone gw@s1k.pw:gwlib
from gwlib import Log
from gwlib import db
from gwlib import util

## Script info
EXE = 'varload'
VERSION = '0.1.12'
## Tag output files with script arguments so we know how the data was generated
FILETAG = reduce(lambda x, y: x + ' ' + y, argv)

def parse_variant_file(fp, build):
    """
    """

    lines = util.parse_generic_file(fp)
    snps = []

    for ln in lines:
        snp = {}
        snp['var_ref_id'] = ln[0]
        snp['var_obs_alleles'] = ln[1]
        snp['var_ma'] = None if ln[2] == '.' else ln[2]
        snp['var_maf'] = None if ln[3] == '.' else ln[3]
        snp['var_clinsig'] = ln[4]

        for ass in ln[5].split('||'):
            ass = ass.split('|')

            ## Skipe irrelevant assembly builds. There should be only one
            ## assembly matching the one we're interested in, so we shouldn't
            ## have to care about overwriting any variables
            if ass[0].lower() not in build:
                continue
            ## Idk why the fuck this is even possible
            if ass[2] == '?':
                continue

            snp['var_chromosome'] = ass[1]
            snp['var_position'] = int(ass[2])
            snp['var_orientation'] = ass[3]

            ## Lmao so the comment above was wrong and there are indeed
            ## instances where the same bulid appears twice. WHO KNOWS THE FUCK
            ## WHY, especially when the second appearance usually has '?' as a
            ## position instead of actual coordinates. Nothing is ever fucking
            ## easy.
            break

        ## Skip things without proper positions
        if 'var_chromosome' not in snp:
            continue

        snps.append(snp)

    return snps

def parse_variant_file_list(fp, build):
    """
    """
    ## 0, var_id; 1, var_ref_id; 2, var_allele; 3, var_chromosome; 
    ## 4, var_position; 5, vt_id; 6, var_ref_cur; 7, var_obs_alleles;
    ## 8, var_ma; 9, var_maf; 10, var_clinsig; 11, gb_id;

    lines = util.parse_generic_file(fp)
    snps = []

    for ln in lines:
        snp = []
        snp.append('\N')
        snp.append(ln[0])
        snp.append('\N')

        for ass in ln[5].split('||'):
            ass = ass.split('|')

            ## Skipe irrelevant assembly builds. There should be only one
            ## assembly matching the one we're interested in, so we shouldn't
            ## have to care about overwriting any variables
            if ass[0].lower() not in build:
                continue
            ## Idk why the fuck this is even possible
            if ass[2] == '?':
                continue

            snp.append(ass[1])
            snp.append((ass[2]))
            #snp['var_orientation'] = ass[3]

            ## Lmao so the comment above was wrong and there are indeed
            ## instances where the same bulid appears twice. WHO KNOWS THE FUCK
            ## WHY, especially when the second appearance usually has '?' as a
            ## position instead of actual coordinates. Nothing is ever fucking
            ## easy.
            break

        snp.append('\N')
        snp.append('t')
        snp.append(ln[1])
        snp.append('\N' if ln[2] == '.' else ln[2])
        snp.append('\N' if ln[3] == '.' else ln[3])
        snp.append(ln[4])
        snp.append(0)

        if len(snp) != 12:
            continue

        snps.append(snp)

    return snps

def print_supported_builds(log):
    """
    Prints the genome build names supported by geneweaver.
    """

    bulids = db.get_genome_builds()

    log.error('Supported genome builds:')

    for build in bulids:
        if build['gb_altref_id']:
            log.error('\t%s/%s' % (build['gb_ref_id'], build['gb_altref_id']))

        else:
            log.error('\t%s' % build['gb_ref_id'])

def get_genome_build_info(build):
    """
    Matches the build ID to the given label.
    """

    bulids = db.get_genome_builds()
    bulid = build.lower()

    for b in bulids:
        if b['gb_ref_id'].lower() == build or\
           b['gb_altref_id'].lower() == build:

               ## Makes label comparison easier later on
               bdict = {}
               bdict[b['gb_ref_id']] = True
               bdict[b['gb_ref_id'].lower()] = True
               bdict[b['gb_altref_id']] = True
               bdict[b['gb_altref_id'].lower()] = True

               return (b['gb_id'], bdict)

    return (None, None)

def get_last_var_id():
    """
    Returns the largest var_id in the database.
    """
    with db.PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT   var_id
            FROM     extsrc.variant
            ORDER BY var_id DESC
            LIMIT    1;
            '''
        )

        res = cursor.fetchone()

        if not res:
            return 0
       
        else:
            return res[0]

def build_snp_string(snp):
    """
    """

    return '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
        snp['var_id'],
        snp['var_ref_id'],
        snp['var_allele'],
        snp['var_chromosome'],
        snp['var_position'],
        snp['vt_id'],
        't',
        snp['var_obs_alleles'],
        '\N' if not snp['var_ma'] else snp['var_ma'],
        '\N' if not snp['var_maf'] else snp['var_maf'],
        snp['var_clinsig'],
        snp['gb_id']
    )

def parse_variant_file_line(ln, build):
    """
    """
    ## 0, var_id; 1, var_ref_id; 2, var_allele; 3, var_chromosome; 
    ## 4, var_position; 5, vt_id; 6, var_ref_cur; 7, var_obs_alleles;
    ## 8, var_ma; 9, var_maf; 10, var_clinsig; 11, gb_id;

    snp = []
    snp.append('\N')
    snp.append(ln[0].strip('rs'))
    snp.append('\N')

    for ass in ln[5].split('||'):
        ass = ass.split('|')

        ## Skip irrelevant assembly builds. There should be only one
        ## assembly matching the one we're interested in, so we shouldn't
        ## have to care about overwriting any variables
        if ass[0].lower() not in build:
            continue
        ## Idk why the fuck this is even possible
        if ass[2] == '?':
            continue

        snp.append(ass[1])
        snp.append((ass[2]))

        ## Lmao so the comment above was wrong and there are indeed
        ## instances where the same bulid appears twice. WHO KNOWS THE FUCK
        ## WHY, especially when the second appearance usually has '?' as a
        ## position instead of actual coordinates. Nothing is ever fucking
        ## easy.
        break

    snp.append('\N')
    snp.append('t')
    snp.append(ln[1])
    snp.append('\N' if ln[2] == '.' else ln[2])
    snp.append('\N' if ln[3] == '.' else ln[3])
    snp.append(ln[4])
    snp.append(0)

    if len(snp) != 12:
        return None

    return snp

def read_and_build_strings(fp, var_id, build, build_id, type_id):
    """
    Simultaenously reads and bulids variant COPY strings to help minimize
    memory usage.
    """

    snps = []

    with open(fp, 'r') as fl:
        for ln in fl:
            ln = ln.strip()

            if not ln:
                continue

            if ln[0] == '#':
                continue

            ln = ln.split('\t')
            ln = parse_variant_file_line(ln, build)

            if not ln:
                continue

            alleles = ln[7].split('/')

            ln[2] = alleles[0]
            ln[5] = str(type_id)
            ln[11] = str(build_id)

            var_id += 1

            ln[0] = str(var_id)

            ## Insertion w/ first allele, the rest use the same var_id
            #snps.append(list(ln))
            snps.append('\t'.join(ln))

            for allele in alleles[1:]:
                ln[2] = allele

                #snps.append(ln)
                snps.append('\t'.join(ln))

    return snps

if __name__ == '__main__':
    from optparse import OptionParser

    ## cmd line shit
    usage = '%s [options] <genome-build> <variant-files>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option(
        '-b', '--builds', action='store_true', dest='builds', 
        help="display supported builds and exit"
    )
    parse.add_option(
        '-c', '--copy', action='store', type='str', dest='copy', 
        help="output data to a file for use with the postgres COPY command"
    )
    parse.add_option(
        '-n', '--neo4j', action='store', type='str', dest='neo4j', 
        help="output data to a file in a format suitable for input into neo4j"
    )
    parse.add_option(
        '--no-commit', action='store_true', dest='nocommit', 
        help="don't commit any database changes (useful for testing)"
    )
    parse.add_option(
        '--verbose', action='store_true', dest='verbose', 
        help='Clutter your screen with output'
    )

    (opts, args) = parse.parse_args(argv)
    log = Log(on=opts.verbose)

    if opts.builds:
        print_supported_builds(log)
        exit()

    if len(args) < 3:
        log.error('[!] You need to provide a genome build')
        log.error('[!] You need to provide a variant file')
        log.error('[!] For a list of supported builds: python %s -b' % args[0])
        log.error('')
        parse.print_help()
        exit()

    (build_id, build_info) = get_genome_build_info(args[1])

    if not build_id or not build_info:
        log.error('[!] The genome build you provided is not supported')
        exit()


    ## All this shit below takes up too much memory. Just leaving here for
    ## posterity but will probably delete soon. This works well on small data
    ## sources but RefSNP will never be a small data source.
    #
    #for vfp in args[2:]:
    #    log.info('[+] Parsing variant file, %s...' % vfp)

    #    snps = parse_variant_file(args[2], build_info)

    #    ## Compared to annovar, NCBI uses more generic type/effect annotations to 
    #    ## describe variants. So we just set all new variants to 'unknown' and then
    #    ## when we encounter annovar flavored variant annotations, we update these
    #    ## effects. It also saves us the trouble of attempting to map NCBI
    #    ## descriptions to annovar ones.
    #    unknown_id = db.get_variant_type_by_effect('unknown')
    #    unknown_id = unknown_id[0]['vt_id']

    #    log.info('[+] Inserting variants...')

    #    ## Variant insertion. A separate variant (but with the same variant ID) is
    #    ## inserted for each allele to keep track of variant-allele-gene 
    #    ## associations.
    #    for snp in snps:
    #        alleles = snp['var_obs_alleles'].split('/')

    #        snp['var_allele'] = alleles[0]
    #        snp['var_ref_cur'] = True
    #        snp['vt_id'] = unknown_id
    #        snp['gb_id'] = build_id

    #        ## Insertion w/ first allele, the rest use the same var_id
    #        var_id = db.insert_variant(snp)

    #        snp['var_id'] = var_id

    #        for allele in alleles[1:]:
    #            snp['var_allele'] = allele

    #            db.insert_variant_with_id(snp)

    #    del snps


    #for vfp in args[2:]:
    #    log.info('[+] Parsing variant file, %s...' % vfp)

    #    start = time.clock()

    #    ## 0, var_id; 1, var_ref_id; 2, var_allele; 3, var_chromosome; 
    #    ## 4, var_position; 5, vt_id; 6, var_ref_cur; 7, var_obs_alleles;
    #    ## 8, var_ma; 9, var_maf; 10, var_clinsig; 11, gb_id;
    #    #snps = parse_variant_file(args[2], build_info)
    #    snps = parse_variant_file_list(args[2], build_info)

    #    ## Compared to annovar, NCBI uses more generic type/effect annotations to 
    #    ## describe variants. So we just set all new variants to 'unknown' and then
    #    ## when we encounter annovar flavored variant annotations, we update these
    #    ## effects. It also saves us the trouble of attempting to map NCBI
    #    ## descriptions to annovar ones.
    #    unknown_id = db.get_variant_type_by_effect('unknown')
    #    unknown_id = unknown_id[0]['vt_id']
    #    #snp_strs = []

    #    log.info('    (%0.1f seconds)' % (time.clock() - start))
    #    log.info('[+] Building SQL COPY strings...')

    #    start = time.clock()
    #    all_snps = []
    #    ## Variant insertion. A separate variant (but with the same variant ID) is
    #    ## inserted for each allele to keep track of variant-allele-gene 
    #    ## associations.
    #    #for snp in snps:
    #    for i in range(len(snps)):
    #        #alleles = snp['var_obs_alleles'].split('/')
    #        alleles = snps[i][7].split('/')

    #        #snp['var_allele'] = alleles[0]
    #        #snp['var_ref_cur'] = True
    #        #snp['vt_id'] = unknown_id
    #        #snp['gb_id'] = build_id
    #        snps[i][2] = alleles[0]
    #        snps[i][5] = str(unknown_id)
    #        snps[i][11] = str(build_id)

    #        last_var_id += 1

    #        snps[i][0] = str(last_var_id)

    #        ## Insertion w/ first allele, the rest use the same var_id
    #        #var_id = db.insert_variant(snp)
    #        #snp_strs.append(build_snp_string(snp))
    #        all_snps.append(snps[i])

    #        for allele in alleles[1:]:
    #            snps[i][2] = allele

    #            all_snps.append(snps[i])

    #            #db.insert_variant_with_id(snp)
    #            #snp_strs.append(build_snp_string(snp))

    #    #del snps

    #    log.info('    (%0.1f seconds)' % (time.clock() - start))
    #    log.info('[+] Finalizing output...')

    #    start = time.clock()
    #    #snp_strs = '\n'.join(snp_strs)
    #    #snps = map(lambda s: '\t'.join(s), snps)
    #    snps = map(lambda s: '\t'.join(s), all_snps)
    #    snps = '\n'.join(snps)
    #    #fl_str = StringIO(snp_strs)
    #    #fl_str = StringIO.StringIO(snps)

    #    log.info('    (%0.1f seconds)' % (time.clock() - start))
    #    log.info('[+] COPYing variant data...')

    #    start = time.clock()

    #    if opts.copy:
    #        if not os.path.exists(opts.copy):
    #            with open(opts.copy, 'w') as fl:
    #                pass

    #        with open(opts.copy, 'a') as fl:
    #            print >> fl, snps

    #    else:
    #        snps = StringIO.StringIO(snps)

    #        with db.PooledCursor() as cursor:
    #            #cursor.copy_from(fl_str, 'extsrc.variant')
    #            cursor.copy_from(snps, 'extsrc.variant')

    var_id = get_last_var_id()
    type_id = db.get_variant_type_by_effect('unknown')
    type_id = type_id[0]['vt_id']

    log.info('[+] Reading and building variant strings...')

    start = time.clock()

    snps = read_and_build_strings(args[2], var_id, build_info, build_id, type_id)

    if not opts.neo4j:
        snps = '\n'.join(snps)

    log.info('    (%0.1f seconds)' % (time.clock() - start))
    log.info('[+] COPYing variant data...')

    start = time.clock()

    if opts.copy:
        if not os.path.exists(opts.copy):
            with open(opts.copy, 'w') as fl:
                pass

        with open(opts.copy, 'a') as fl:
            print >> fl, snps

    elif opts.neo4j:

        var_id = 0

        with open(opts.neo4j, 'w') as fl:
            print >> fl, 'id,ref_id,allele,chromosome,position,effect,ref_cur,obs_alleles,ma,maf,clinsig,build'

            for snp in snps:
                snp = snp.split('\t')
                out = ''
                out = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
                    var_id,
                    snp[1],
                    snp[2],
                    snp[3],
                    snp[4],
                    'unknown',
                    'true',
                    snp[7],
                    snp[8] if snp[8] != '\N' else '',
                    snp[9] if snp[9] != '\N' else '',
                    args[1]
                )
                var_id += 1

                print >> fl, out

            ## 0, var_id; 1, var_ref_id; 2, var_allele; 3, var_chromosome; 
            ## 4, var_position; 5, vt_id; 6, var_ref_cur; 7, var_obs_alleles;
            ## 8, var_ma; 9, var_maf; 10, var_clinsig; 11, gb_id;

    else:
        snps = StringIO.StringIO(snps)

        with db.PooledCursor() as cursor:
            cursor.copy_from(snps, 'extsrc.variant')

    if not opts.nocommit and not opts.copy and not opts.neo4j:
        log.info('[+] Committing changes, no turning back now...')

        db.commit()

    log.info('    (%0.1f seconds)' % (time.clock() - start))
    log.info('[+] Done!')
    log.info('')

    if opts.copy:
        log.error('Looks like you generated a COPY formatted file.')
        log.error('You can insert that data into the GW DB like so:')
        log.error((
            "    echo \"COPY extsrc.variant FROM '%s';\" " 
            "| psql geneweaver odeadmin"
        ) % opts.copy)

