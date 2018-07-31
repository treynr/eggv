
INSERT INTO odestatic.genome_build
    (gb_ref_id, gb_altref_id, gb_description, sp_id)
VALUES
    ('mm10', 'GRCm38', 'Genome Reference Consortium Mouse Build 38', (SELECT sp_id FROM odestatic.species WHERE sp_name = 'Mus musculus'));

INSERT INTO odestatic.genome_build
    (gb_ref_id, gb_altref_id, gb_description, sp_id)
VALUES
    ('hg38', 'GRCh38', 'Genome Reference Consortium Human Build 38', (SELECT sp_id FROM odestatic.species WHERE sp_name = 'Homo sapiens'));

