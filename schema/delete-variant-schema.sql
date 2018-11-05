
DROP TABLE extsrc.variant;
DROP TABLE production.variant_file;
DROP TABLE odestatic.variant_type;
DROP TABLE extsrc.variant_info;
DROP TABLE extsrc.variant_merge;
DROP TABLE odestatic.genome_build;

DELETE 
FROM odestatic.genedb
WHERE gdb_name = 'Variant' AND
      gdb_shortname = 'variant';

