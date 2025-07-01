-- A collection of convenience views for UMLS data

-- Selecting only child relationships FROM mrrel
CREATE TABLE IF NOT EXISTS umls__mrrel_drug_is_a AS
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
    str
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
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c
WHERE
    c.tty IN ('HT')
    AND r.cui1 = 'C2880081'
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_block AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
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
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_block AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND length(c.code) = 3
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_subcategory_1 AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_category AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND length(c.code) = 5
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui;


CREATE TABLE IF NOT EXISTS umls__icd10_subcategory_2 AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_subcategory_1 AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND length(c.code) = 6
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui;

CREATE TABLE IF NOT EXISTS umls__icd10_subcategory_3 AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_subcategory_2 AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND length(c.code) = 7
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_extension AS
SELECT DISTINCT
    r.rui,
    r.cui1,
    r.cui2,
    c.tty,
    c.code,
    c.str
FROM umls__mrrel_icd10cm AS r,
    umls__mrconso_icd10cm AS c,
    umls__icd10_subcategory_1 AS par
WHERE
    c.tty IN ('HT', 'PT')
    AND length(c.code) = 8
    AND r.rel = 'CHD'
    AND r.cui1 = par.cui2
    AND r.cui2 = c.cui
ORDER BY c.code ASC;

CREATE TABLE IF NOT EXISTS umls__icd10_tree AS
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    2 AS depth
FROM umls__icd10_chapter
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    3 AS depth
FROM umls__icd10_block
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    4 AS depth
FROM umls__icd10_category
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    4 AS depth
FROM umls__icd10_subcategory_1
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    5 AS depth
FROM umls__icd10_subcategory_2
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    6 AS depth
FROM umls__icd10_subcategory_3
UNION ALL
SELECT
    rui,
    cui1,
    cui2,
    tty,
    code,
    str,
    7 AS depth
FROM umls__icd10_extension;

CREATE TABLE umls__icd10_hierarchy AS
WITH chapter AS (
    SELECT
        code AS chapter_code,
        str AS chapter_str,
        cui2
    FROM umls__icd10_tree
    WHERE depth = 2
),

block AS (
    SELECT
        p.chapter_code,
        p.chapter_str,
        c.code AS block_code,
        c.str AS block_str,
        c.cui2
    FROM chapter AS p
    LEFT JOIN umls__icd10_tree AS c
        ON p.cui2 = c.cui1
),

category AS (
    SELECT
        p.chapter_code,
        p.chapter_str,
        p.block_code,
        p.block_str,
        c.code AS category_code,
        c.str AS category_str,
        c.cui2
    FROM block AS p
    LEFT JOIN umls__icd10_tree AS c
        ON p.cui2 = c.cui1
),

subcategory_1 AS (
    SELECT
        p.chapter_code,
        p.chapter_str,
        p.block_code,
        p.block_str,
        p.category_code,
        p.category_str,
        c.code AS subcategory_1_code,
        c.str AS subcategory_1_str,
        c.cui2
    FROM category AS p
    LEFT JOIN umls__icd10_tree AS c
        ON p.cui2 = c.cui1
        -- From here on out, we need to filter out some circular refs in UMLS,
        -- so we'll start looking for codes of specified lengths
        AND length(c.code) = 5
        AND c.code LIKE concat(p.category_code, '%')
),

subcategory_2 AS (
    SELECT
        p.chapter_code,
        p.chapter_str,
        p.block_code,
        p.block_str,
        p.category_code,
        p.category_str,
        p.subcategory_1_code,
        p.subcategory_1_str,
        c.code AS subcategory_2_code,
        c.str AS subcategory_2_str,
        c.cui2
    FROM subcategory_1 AS p
    LEFT JOIN umls__icd10_tree AS c
        ON
            p.cui2 = c.cui1
            AND length(c.code) = 6
            AND c.code LIKE concat(p.subcategory_1_code, '%')
),

subcategory_3 AS (
    SELECT
        p.chapter_code,
        p.chapter_str,
        p.block_code,
        p.block_str,
        p.category_code,
        p.category_str,
        p.subcategory_1_code,
        p.subcategory_1_str,
        p.subcategory_2_code,
        p.subcategory_2_str,
        c.code AS subcategory_3_code,
        c.str AS subcategory_3_str,
        c.cui2
    FROM subcategory_2 AS p
    LEFT JOIN umls__icd10_tree AS c
        ON
            p.cui2 = c.cui1
            AND length(c.code) = 7
            AND c.code LIKE concat(p.subcategory_2_code, '%')
)


SELECT
    p.chapter_code,
    p.chapter_str,
    p.block_code,
    p.block_str,
    p.category_code,
    p.category_str,
    p.subcategory_1_code,
    p.subcategory_1_str,
    p.subcategory_2_code,
    p.subcategory_2_str,
    p.subcategory_3_code,
    p.subcategory_3_str,
    c.code AS extension_code,
    c.str AS extension_str,
    coalesce(
        c.code,
        p.subcategory_3_code,
        p.subcategory_2_code,
        p.subcategory_1_code,
        p.category_code
    ) AS leaf_code
FROM subcategory_3 AS p
LEFT JOIN umls__icd10_tree AS c
    ON
        p.cui2 = c.cui1
        AND length(c.code) = 8
        AND c.code LIKE concat(p.subcategory_1_code, '%')
