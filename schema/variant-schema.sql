-- This code is for the creation of gene tables related to addiding variants to geneweaver
-- It should reflect the ER diagram at http://cs.ecs.baylor.edu/~baker/GW-ER-20180201.svg

CREATE TABLE odestatic.genome_build
(
    gb_id SERIAL PRIMARY KEY NOT NULL,
    gb_ref_id VARCHAR NOT NULL,
    gb_altref_id VARCHAR,
    gb_description VARCHAR,
    sp_id INT NOT NULL,
    CONSTRAINT genome_build_species_sp_id_fk FOREIGN KEY (sp_id) REFERENCES species (sp_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX genome_build_gb_id_uindex ON odestatic.genome_build (gb_id);

COMMENT ON 
TABLE odestatic.genome_build 
IS 'Supported genome builds for the variant table';

COMMENT ON 
COLUMN odestatic.genome_build.gb_ref_id
IS 'The UCSC genome assembly ID. e.g. hg38, mm10, etc.';

COMMENT ON 
COLUMN odestatic.genome_build.gb_altref_id
IS 'Any other alternate genome ID. e.g. GRCh38, GRCm38, etc.';

COMMENT ON 
COLUMN odestatic.genome_build.gb_description
IS 'A description of the genome, usually just taken from NCBI.';

COMMENT ON 
COLUMN odestatic.genome_build.sp_id
IS 'The species this genome belongs to.';

CREATE TABLE extsrc.variant_info
(
    vri_id BIGSERIAL PRIMARY KEY NOT NULL,
    vri_chromosome VARCHAR,
    vri_position INT,
    gb_id INT NOT NULL,
    CONSTRAINT variant_info_genome_build_gb_id_fk FOREIGN KEY (gb_id) REFERENCES genome_build (gb_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX variant_info_vri_id_uindex ON extsrc.variant_info (vri_id);

CREATE INDEX variant_info_vri_chromosome_vri_position_index 
ON extsrc.variant_info (vri_chromosome, vri_position);

COMMENT ON 
TABLE extsrc.variant_info 
IS 'This table connects varients with genome builds and includes genome positions';

COMMENT ON 
COLUMN extsrc.variant_info.vri_chromosome
IS 'Chromosme the variant is found on';

COMMENT ON 
COLUMN extsrc.variant_info.vri_position
IS 'Position (bp) of the variant';

COMMENT ON 
COLUMN extsrc.variant_info.gb_id
IS 'Genome build for this variant';

CREATE TABLE odestatic.variant_type
(
    vt_id SERIAL PRIMARY KEY NOT NULL,
    vt_ref_id VARCHAR,
    vt_effect VARCHAR NOT NULL,
    vt_impact VARCHAR,
    vt_description VARCHAR
);

CREATE UNIQUE INDEX variant_type_vt_id_uindex ON odestatic.variant_type (vt_id);

COMMENT ON TABLE odestatic.variant_type IS 'controlled vocabulary for variant type';

COMMENT ON 
COLUMN odestatic.variant_type.vt_ref_id
IS 'Sequence Ontology (SO) reference ID for this variant type';

COMMENT ON 
COLUMN odestatic.variant_type.vt_effect
IS 'Sequence effect of this variant';

COMMENT ON 
COLUMN odestatic.variant_type.vt_impact
IS 'Impact this variant has on resulting sequence';

COMMENT ON 
COLUMN odestatic.variant_type.vt_description
IS 'A small description of this variant type.';

CREATE TABLE extsrc.variant
(
    var_id BIGSERIAL PRIMARY KEY NOT NULL,
    var_ref_id BIGINT DEFAULT 0 NOT NULL,
    var_allele VARCHAR,
    vt_id INT NOT NULL,
    var_ref_cur BOOLEAN DEFAULT TRUE ,
    var_obs_alleles VARCHAR,
    var_ma VARCHAR,
    var_maf FLOAT,
    var_clinsig VARCHAR,
    vri_id INT NOT NULL,
    CONSTRAINT variant_variant_info_vri_id_fk FOREIGN KEY (vri_id) REFERENCES variant_info (vri_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT variant_odestatic_variant_type_vt_id_fk FOREIGN KEY (vt_id) REFERENCES odestatic.variant_type (vt_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);

CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);

CREATE INDEX variant_vri_id_index ON extsrc.variant (vri_id);

COMMENT ON 
COLUMN extsrc.variant.var_ref_id
IS 'Similar to an ode_ref_id, a reference ID from an external source. This will almost always be an rs ID.';

COMMENT ON 
COLUMN extsrc.variant.var_allele
IS 'The allele sequence of this particular variant.';

COMMENT ON 
COLUMN extsrc.variant.vt_id
IS 'Reference to the table indicating the variant type.';

COMMENT ON 
COLUMN extsrc.variant.var_ref_cur
IS 'Indicates whether the associated ref_id is current (true) or has been superseded by another (false)';

COMMENT ON 
COLUMN extsrc.variant.var_obs_alleles
IS 'All observed alleles this variant has. Listed similarly to NCBI where each allele is separated by a /. e.g. A/T.';

COMMENT ON 
COLUMN extsrc.variant.var_ma
IS 'The minor allele of this variant.';

COMMENT ON 
COLUMN extsrc.variant.var_maf
IS 'The minor allele frequency.';

COMMENT ON 
COLUMN extsrc.variant.var_clinsig
IS 'Clinical significance of this variant, provided by NCBI ClinVar. Possible values: https://www.ncbi.nlm.nih.gov/clinvar/docs/clinsig/';

COMMENT ON 
COLUMN extsrc.variant.vri_id
IS 'Variant info reference which contains genome build and genomic coordinates.';

CREATE TABLE production.variant_file
(
    vf_id BIGSERIAL PRIMARY KEY NOT NULL,
    gs_id BIGINT NOT NULL,
    vf_contents TEXT NOT NULL,
    vf_size BIGINT,
    vf_comments VARCHAR,
    CONSTRAINT variant_file_geneset_gs_id_fk FOREIGN KEY (gs_id) REFERENCES geneset (gs_id)
);

CREATE UNIQUE INDEX variant_file_vf_id_uindex ON production.variant_file (vf_id);

COMMENT ON TABLE production.variant_file IS 'File to store raw variant data. Variant version of file table';

CREATE TABLE extsrc.variant_merge
(
    vm_ref_old BIGINT NOT NULL,
    vm_ref_new BIGINT NOT NULL,
    gb_id INT NOT NULL,
    CONSTRAINT variant_merge_genome_build_gb_id_fk FOREIGN KEY (gb_id) REFERENCES genome_build (gb_id)
);

CREATE INDEX variant_merge_vm_ref_old_index ON extsrc.variant_merge (vm_ref_old);

COMMENT ON TABLE extsrc.variant_merge IS 'Reference SNP identifiers (rsID) that have been deprecated and merged into another rsID';

COMMENT ON
COLUMN extsrc.variant_merge.vm_ref_old
IS 'Deprecated rsID';

COMMENT ON
COLUMN extsrc.variant_merge.vm_ref_new
IS 'Current rsID';

COMMENT ON
COLUMN extsrc.variant_merge.gb_id
IS 'Genome build';

