-- A collection of convenience views for UMLS data

-- Selecting only child relationships FROM mrrel
CREATE TABLE IF NOT EXISTS umls__mrrel_is_a AS
SELECT
    cui1,
    aui1,
    stype1,
    rel,
    cui2,
    aui2,
    stype2,
    rela,
    rui,
    srui,
    sab,
    sl,
    rg,
    dir,
    suppress,
    cvf
FROM umls__mrrel
WHERE (
    rel = 'CHD'
    OR rela IN (
        'isa',
        'tradename_of',
        'has_tradename',
        'has_basis_of_strength_substance'
    )
)
AND rel NOT IN ('RB', 'PAR');

-- Selecting only drug-related concepts FROM mrconso
CREATE TABLE IF NOT EXISTS umls__mrconso_drugs AS
SELECT
    cui,
    lat,
    ts,
    lui,
    stt,
    sui,
    ispref,
    aui,
    saui,
    scui,
    sdui,
    sab,
    tty,
    code,
    str
FROM umls__mrconso
WHERE sab IN (
    'ATC',
    'CVX',
    'DRUGBANK',
    'GS',
    'MED-RT',
    'MMSL',
    'MMX',
    'MTHCMSFRF',
    'MTHSPL',
    'NDDF', 'RXNORM', 'SNOMEDCT_US', 'USP', 'VANDF'
);

-- ICD-10CM convenience views

CREATE TABLE IF NOT EXISTS umls__mrconso_icd10cm AS
SELECT
    cui,
    lat,
    ts,
    lui,
    stt,
    sui,
    ispref,
    aui,
    saui,
    scui,
    sdui,
    sab,
    tty,
    code,
    str,
    length(code) AS code_len
FROM umls__mrconso
WHERE sab = 'ICD10CM';

CREATE TABLE IF NOT EXISTS umls__mrrel_icd10cm AS
SELECT
    cui1,
    aui1,
    stype1,
    rel,
    cui2,
    aui2,
    stype2,
    rela,
    rui,
    srui,
    sab,
    sl,
    rg,
    dir,
    suppress,
    cvf
FROM umls__mrrel
WHERE sab = 'ICD10CM';

-- The following views slice out individual ICD layers.
-- This lines up with how a human might traverse the nomenclature to find
-- a set of codes related to a specific condition.

CREATE TABLE IF NOT EXISTS umls__icd10_chapter AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code_len,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c
WHERE
    c.tty IN ('HT')
    AND c.code LIKE '%-%'
    AND r.cui1 = 'C2880081'
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_block AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code_len,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_chapter AS par
WHERE
    c.tty IN ('HT')
    AND r.rel = 'CHD'
    AND c.code LIKE '%-%'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_category AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code_len,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_block AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND c.code_len = 3
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_code AS

WITH code_5 AS (
    SELECT DISTINCT
        r.rui,
        r.cui1,
        r.cui2,
        c.tty,
        c.code_len,
        c.code,
        c.str
    FROM umls__mrrel_icd10cm AS r,
        umls__mrconso_icd10cm AS c,
        umls__icd10_category AS par
    WHERE
        c.tty IN ('HT', 'PT')
        AND c.code LIKE '%.%'
        AND (c.code_len = 5 OR c.code LIKE '%.%X%')
        AND r.rel = 'CHD'
        AND r.cui1 = par.cui2
        AND r.cui2 = c.cui
),

code_6 AS (
    SELECT DISTINCT
        r.rui,
        r.cui1,
        r.cui2,
        c.tty,
        c.code_len,
        c.code,
        c.str
    FROM umls__mrrel_icd10cm AS r,
        umls__mrconso_icd10cm AS c,
        code_5 AS par
    WHERE
        c.tty IN ('HT', 'PT')
        AND c.code LIKE '%.%'
        AND (c.code_len = 6 OR c.code LIKE '%.%X%')
        AND r.rel = 'CHD'
        AND r.cui1 = par.cui2
        AND r.cui2 = c.cui
),

code_7 AS (
    SELECT DISTINCT
        r.rui,
        r.cui1,
        r.cui2,
        c.tty,
        c.code_len,
        c.code,
        c.str
    FROM umls__mrrel_icd10cm AS r,
        umls__mrconso_icd10cm AS c,
        code_6 AS par
    WHERE
        c.tty IN ('HT', 'PT')
        AND c.code LIKE '%.%'
        AND (c.code_len = 7 OR c.code LIKE '%.%X%')
        AND c.code LIKE '%.%'
        AND r.rel = 'CHD'
        AND r.cui1 = par.cui2
        AND r.cui2 = c.cui
    ORDER BY c.code ASC
),

code_8 AS (
    SELECT DISTINCT
        r.rui,
        r.cui1,
        r.cui2,
        c.tty,
        c.code_len,
        c.code,
        c.str
    FROM umls__mrrel_icd10cm AS r,
        umls__mrconso_icd10cm AS c,
        code_7 AS par
    WHERE
        c.tty IN ('HT', 'PT')
        AND c.code LIKE '%.%'
        AND (c.code_len = 8 OR c.code LIKE '%.%X%')
        AND c.code LIKE '%.%'
        AND r.rel = 'CHD'
        AND r.cui1 = par.cui2
        AND r.cui2 = c.cui
    ORDER BY c.code ASC
)

SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    5 AS depth
FROM code_5
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    6 AS depth
FROM code_6
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    7 AS depth
FROM code_7
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    8 AS depth
FROM code_8;

CREATE OR REPLACE VIEW umls__icd10_tree AS
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    2 AS depth
FROM umls__icd10_chapter
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    3 AS depth
FROM umls__icd10_block
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    4 AS depth
FROM umls__icd10_category
UNION
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code_len,
    code,
    str,
    depth
FROM umls__icd10_code
