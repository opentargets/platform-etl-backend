# Current Target (Gene) step overview

## Data pipeline

1. Step is triggered by `--gen`. 

     1. Step has dependency on reactome step (--rea)

2. Gene is managed by `GeneManager` (GeneData.py) using a plugin system. Step 1 instantiates the GeneManager and calls `merge_all` which controls the step. 

     1. The order of plugin execution is important and is specified in the `input_mrtarget.data.yml` file under `gene-data-plugin-names`
     2. Each plugin has a method `merge_data` which is called with the following command: 

  ```
  plugin.plugin_object.merge_data(self.genes, es, None,                 	self.data_config, self.es_config)  
  ```

- `gene` is a `GeneSet`, a wrapper over a dictionary. It is specified in `GeneData.py`. The keys to check for inclusion are `ensembl_gene_id` with fallbacks of `hgnc_id` and `entrez_gene_id`.

3. Each plugin in called and progressively build up the `GeneSet`
4. `merge_all` adds additional data to each gene with `_create_suggestions` and `_create_facets` methods.
5. All genes are loaded into Elasticsearch.

## GeneManager plugins (order significant!)

### HGNC (`hgnc.py`)

1. Loads data from data configuration location `hgnc_complete_set`
2. Creates new `Gene` for each item if there is an _ensembl_gene_id_ field. Sets:
   - ensembl_gene_id
   - hgnc_id
   - approved_symbol
   - approved_name
   - status
   - locus_group
   - previous_symbols
   - previous_names
   - symbol_synonyms
   - name_synonyms
   - enzyme_ids
   - entrez_gene_id
   - refseq_ids
   - gene_family_tag
   - gene_family_description
   - ccds_ids
   - vega_ids
   - uniprot_ids
   - pubmed_id
3. Adds new `Gene` to `GeneSet`

### Orthologs (`orthologs.py`)

1. Loads data from config `hgnc_orthologs_species` and `hgnc_orthologs`
2. If gene is present in GeneSet call `add_orthog_data_to_gene` to update Gene fields:
   - orthog

### Ensembl

1. Loads data from config `ensembl_filename`
2. Gets gene if already present in GeneSet otherwise creates new Gene
3. Calls `load_ensembl_data` to update Gene fields:
   1. is_active_in_ensembl
   2. ensembl_gene_id
   3. ensembl_assembly_name
   4. biotype
   5. ensembl_description
   6. gene_end
   7. gene_start
   8. strand
   9. chromosome
   10. ensembl_external_name
   11. ensembl_gene_version
   12. cytobands
   13. emsembl_release
   14. is_ensemble_reference
4. **Removes all genes from geneSet which are not `is_ensembl_reference`**

### Uniprot

1. Load uniprot data from config `uniprot_uri` (https://storage.googleapis.com/open-targets-data-releases/20.06/input/annotation-files/uniprot-2020-06-01.xml.gz)
2. For each row in data:
   1. Call `load_uniprot_entry` to update gene fields:
    ~~1. uniprot_id - first accession (dp UniprotIO.py ln 507)~~
      2. is_in_swissprot - this is always true (dp uniprot.py ln 35)
      3. dbxrefs (updates what is already there)
    ~~4. uniprot_accessions~~
      5. uniprot_keywords (not used in ETL)
    ~~6. uniprot_function~~
    ~~7. uniprot_similarity~~
    ~~8. uniprot_subunit~~
    ~~9. uniprot_subcellularlocation_location~~
    ~~10. uniprot_pathway~~
    ~~11. approved_symbol - gene_name_primary in UniprotIO~~
    ~~12. symbol_synonyms - gene_name_synonym in UniprotIO~~
      13. name_synonyms
        - recommended_name
        - alternative_name
      14. go
      15. reactome
          1. This calls out to `reactome` in Elasticsearch and updates based on the results. This is why the gene step has a dependency on reactome. 
      16. pdb
      17. chembl
      18. drugbank
      19. pfam
      20. interpro
      
#### Raw input (asterisk next to used fields)

```

    /*
    res51: Array[String] = Array(
        "_created",
        "_dataset",
        "_modified",
        "_version",
     *  "accession",
     *  "comment",
        "dbReference",
        "evidence",
        "feature",
     *  "gene",
        "geneLocation",
        "keyword",
        "name",
        "organism",
     *  "protein",
        "proteinExistence",
        "reference",
        "sequence"
      )
     */
```

### ChEMBL

- Has dependency on `uniprot_accessions` so _Uniprot_ plugin outputs.
- Loads data from `chembl_mechanism`, `chembl_component`, `chembl_target`, `chembl_protein`, `chembl_molecule` but only seems to use molecule and target?

1. For each gene updates
   - drugs
   - protein_classification

### MousePhenotypes

- Uses `ontology_mp` in config as input
- Uses _external library_ [opentargets_ontologyutils](https://github.com/opentargets/ontology-utils) to load in data

1. For each gene updates:
   - mouse_phenotypes

### Hallmarks

- Uses `hallmark` in config as input
- Requires `approved_symbol` be set on gene (dependent on HGNC or uniprot step)

1. For each gene updates:
   1. hallmarks

### CancerBiomarkers

- Uses input `cancerbiomarkers`

- Requires `approved_symbol` be set on gene (dependent on HGNC or uniprot step)

1. For each gene updates:
   1. cancerbiomarkers

### ChemicalProbes

- Uses input `chemical_probes_1` and `chemical_probes_2`

- Requires `approved_symbol` be set on gene (dependent on HGNC or uniprot step)

1. For each gene updates:
   1. chemicalprobes

### Tractability

- Uses input `tractability`

1. For each gene updates:
   1. tractability

### Safety

- Requires `approved_symbol` be set on gene (dependent on HGNC or uniprot step)
- Uses inputs `safety` and `experimental_toxicity`

1. For each gene updates:
   1. safety 

## ETL

The relevant step in the ETL is `target`.

### Inputs
```
  target {
       format = "parquet"
       path = "gs://ot-snapshots/parquet/20.11/gene-data"
    }
    tep {
      format = "json"
      path = "gs://ot-snapshots/etl/inputs/20.11/jsonl/tep-2020-11-10.json"
```

#### Target input (truncated)

For complete listing, see _target-input.md_

Fields actually used in ETL:
```
         "id" - ???
         "approved_name as approvedName", - hgnc
         "approved_symbol as approvedSymbol", - hgnc
         "biotype as bioType",
         "hgnc_id as hgncId", - hgnc
         "hallmarks as hallMarksRoot", - hallmarks plugin
         "tractability as tractabilityRoot", - tractability plugin
         "safety as safetyRoot", - safety plugin
         "chemicalprobes as chemicalProbes", - chemical probes plugin
         "go as goRoot", - uniprot plugin
         "reactome", - uniprot plugin but calling to Elasticsearch
         "name_synonyms as nameSynonyms", - hgnc and uniprot
         "symbol_synonyms as symbolSynonyms", - hgnc
         "chromosome, - ensembl
          gene_start as start, - ensembl
          gene_end as end, -enseml
          strand, - ensembl
          uniprot_id as id, - uniprot (done)
          uniprot_accessions as accessions,- uniprot (done)
          uniprot_function as functions,- uniprot (done)
          uniprot_pathway as pathways,- uniprot (done)
          uniprot_similarity as similarities,- uniprot (done)
          uniprot_subcellular_location as subcellularLocations,- uniprot (done)
          uniprot_subunit as subunits,- uniprot (done) 
          protein_classification.chembl as classes - chembl plugin
```

```
root
 |-- _private: struct (nullable = true)
 |    |-- facets: struct (nullable = true)
 |    |    |-- reactome: struct (nullable = true)
 |    |    |    |-- pathway_code: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- pathway_type_code: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |-- suggestions: struct (nullable = true)
 |    |    |-- input: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- output: string (nullable = true)
 |    |    |-- payload: struct (nullable = true)
 |    |    |    |-- gene_id: string (nullable = true)
 |    |    |    |-- gene_name: string (nullable = true)
 |    |    |    |-- gene_symbol: string (nullable = true)
 |-- alias_name: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- alias_symbol: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- approved_name: string (nullable = true)
 |-- approved_symbol: string (nullable = true)
 |-- biotype: string (nullable = true)
 |-- cancerbiomarkers: array (nullable = true)
 |   < cancer biomarkers struct > 
 |-- ccds_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- chembl: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- chemicalprobes: struct (nullable = true)
 |   < probes struct > 
 |-- chromosome: string (nullable = true)
 |-- cytobands: string (nullable = true)
 |-- dbxrefs: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- drugbank: array (nullable = true)
 |   < drugbank struct > 
 |-- drugs: struct (nullable = true)
 |    |-- chembl_drugs: array (nullable = true)
 |   < drugs struct > 
 |-- ensembl_assembly_name: string (nullable = true)
 |-- ensembl_description: string (nullable = true)
 |-- ensembl_external_name: string (nullable = true)
 |-- ensembl_gene_id: string (nullable = true)
 |-- ensembl_gene_version: long (nullable = true)
 |-- ensembl_release: string (nullable = true)
 |-- entrez_gene_id: string (nullable = true)
 |-- enzyme_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- gene_end: long (nullable = true)
 |-- gene_family_description: string (nullable = true)
 |-- gene_family_tag: string (nullable = true)
 |-- gene_start: long (nullable = true)
 |-- go: array (nullable = true)
 | < gene ontology structure > 
 |-- hallmarks: struct (nullable = true)
 | < hallmarks structure > 
 |-- hgnc_id: string (nullable = true)
 |-- id: string (nullable = true)
 |-- interpro: array (nullable = true)
 |  < interpro struct > 
 |-- is_active_in_ensembl: boolean (nullable = true)
 |-- is_ensembl_reference: boolean (nullable = true)
 |-- is_in_swissprot: boolean (nullable = true)
 |-- locus_group: string (nullable = true)
 |-- mouse_phenotypes: string (nullable = true)
 |-- name_synonyms: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- ortholog: struct (nullable = true)
 |    |-- chimpanzee: array (nullable = true)
 |  < ortholog structure > 
 |-- pdb: array (nullable = true)
 |   <pdb structure > 
 |-- pfam: array (nullable = true)
 |   <pfam structure > 
 |-- previous_names: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- previous_symbols: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- protein_classification: struct (nullable = true)
 |    |-- chembl: array (nullable = true)
 |      <chembl protein classification structure >
 |-- pubmed_ids: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- reactome: array (nullable = true)
 |  <reactome structure > 
 |-- refseq_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- safety: struct (nullable = true)
 |   < safety structure > 
 |-- status: string (nullable = true)
 |-- strand: long (nullable = true)
 |-- symbol_synonyms: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tractability: struct (nullable = true)
 |   < tractability structure > 
 |-- uniprot_accessions: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_function: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_id: string (nullable = true)
 |-- uniprot_keywords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_mapping_date: string (nullable = true)
 |-- uniprot_pathway: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_similarity: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_subcellular_location: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- uniprot_subunit: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- vega_ids: string (nullable = true)
```

#### Tep input

```
root
 |-- Ensembl_id: string (nullable = true)
 |-- OT_Target_name: string (nullable = true)
 |-- URI: string (nullable = true)
```

### Output from ETL 20.11

```
root
 |-- approvedName: string (nullable = true)
 |-- approvedSymbol: string (nullable = true)
 |-- bioType: string (nullable = true)
 |-- chemicalProbes: struct (nullable = true)
 |    |-- portalprobes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- chemicalprobe: string (nullable = true)
 |    |    |    |-- gene: string (nullable = true)
 |    |    |    |-- note: string (nullable = true)
 |    |    |    |-- sourcelinks: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- link: string (nullable = true)
 |    |    |    |    |    |-- source: string (nullable = true)
 |    |-- probeminer: struct (nullable = true)
 |    |    |-- link: string (nullable = true)
 |-- genomicLocation: struct (nullable = true)
 |    |-- chromosome: string (nullable = true)
 |    |-- end: long (nullable = true)
 |    |-- start: long (nullable = true)
 |    |-- strand: long (nullable = true)
 |-- go: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- value: struct (nullable = true)
 |    |    |    |-- evidence: string (nullable = true)
 |    |    |    |-- project: string (nullable = true)
 |    |    |    |-- term: string (nullable = true)
 |-- hallMarks: struct (nullable = true)
 |    |-- attributes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- attribute_name: string (nullable = true)
 |    |    |    |-- description: string (nullable = true)
 |    |    |    |-- pmid: long (nullable = true)
 |    |-- cancer_hallmarks: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- description: string (nullable = true)
 |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- pmid: long (nullable = true)
 |    |    |    |-- promote: boolean (nullable = true)
 |    |    |    |-- suppress: boolean (nullable = true)
 |    |-- function_summary: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- description: string (nullable = true)
 |    |    |    |-- pmid: long (nullable = true)
 |-- hgncId: string (nullable = true)
 |-- id: string (nullable = true)
 |-- nameSynonyms: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- proteinAnnotations: struct (nullable = true)
 |    |-- accessions: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- classes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- l1: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- l2: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- l3: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- l4: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- l5: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- l6: struct (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- label: string (nullable = true)
 |    |-- functions: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- id: string (nullable = true)
 |    |-- pathways: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- similarities: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- subcellularLocations: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- subunits: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- reactome: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- safety: struct (nullable = true)
 |    |-- adverse_effects: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- activation_effects: struct (nullable = true)
 |    |    |    |    |-- acute_dosing: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |    |-- general: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |-- inhibition_effects: struct (nullable = true)
 |    |    |    |    |-- acute_dosing: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |    |-- chronic_dosing: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |    |-- developmental: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |    |-- general: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |-- organs_systems_affected: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |-- references: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- pmid: long (nullable = true)
 |    |    |    |    |    |-- ref_label: string (nullable = true)
 |    |    |    |    |    |-- ref_link: string (nullable = true)
 |    |-- experimental_toxicity: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- data_source: string (nullable = true)
 |    |    |    |-- data_source_reference_link: string (nullable = true)
 |    |    |    |-- experiment_details: struct (nullable = true)
 |    |    |    |    |-- assay_description: string (nullable = true)
 |    |    |    |    |-- assay_format: string (nullable = true)
 |    |    |    |    |-- assay_format_type: string (nullable = true)
 |    |    |    |    |-- cell_short_name: string (nullable = true)
 |    |    |    |    |-- tissue: string (nullable = true)
 |    |-- safety_risk_info: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- organs_systems_affected: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- mapped_term: string (nullable = true)
 |    |    |    |    |    |-- term_in_paper: string (nullable = true)
 |    |    |    |-- references: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- pmid: long (nullable = true)
 |    |    |    |    |    |-- ref_label: string (nullable = true)
 |    |    |    |    |    |-- ref_link: string (nullable = true)
 |    |    |    |-- safety_liability: string (nullable = true)
 |-- symbolSynonyms: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tep: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- uri: string (nullable = true)
 |-- tractability: struct (nullable = true)
 |    |-- antibody: struct (nullable = true)
 |    |    |-- buckets: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- categories: struct (nullable = true)
 |    |    |    |-- clinical_precedence: double (nullable = true)
 |    |    |    |-- predicted_tractable_high_confidence: double (nullable = true)
 |    |    |    |-- predicted_tractable_med_low_confidence: double (nullable = true)
 |    |    |-- top_category: string (nullable = true)
 |    |-- other_modalities: struct (nullable = true)
 |    |    |-- buckets: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- categories: struct (nullable = true)
 |    |    |    |-- clinical_precedence: double (nullable = true)
 |    |-- smallmolecule: struct (nullable = true)
 |    |    |-- buckets: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- categories: struct (nullable = true)
 |    |    |    |-- clinical_precedence: double (nullable = true)
 |    |    |    |-- discovery_precedence: double (nullable = true)
 |    |    |    |-- predicted_tractable: double (nullable = true)
 |    |    |-- high_quality_compounds: long (nullable = true)
 |    |    |-- small_molecule_genome_member: boolean (nullable = true)
 |    |    |-- top_category: string (nullable = true)

```

