[![Build Status](https://travis-ci.com/opentargets/platform-etl-backend.svg?branch=master)](https://travis-ci.com/opentargets/platform-etl-backend)

# Open Targets Post-Pipeline ETL process

OpenTargets ETL pipeline to process Pipeline output in order to obtain a new API shaped entities. For the file 
`platformDataBackend.sc`

## platformDataBackend.sc

### Requirements

1. OpenJDK 8/11
2. scala 2.12.x (through SDKMAN is simple) [Scala Doc](https://www.scala-lang.org/api/2.12.12/index.html)
3. ammonite REPL
4. Input resources from PIS (src/main/resources/conference.conf)

#### Notes on Java version

Either Java 8 or 11 can be used to build and run the project, but if you intend to use the compiled jar file on
_Dataproc_ you must use Java 8. To avoid this problem altogether, do not use native Java methods unless strictly
necessary.

### Configuration

There is a directory called `configuration` which versions all the release configuration files. To repeat a release use
the appropriate configuration file.

Add to your run either commandline or sbt task Intellij IDEA `-Dconfig.file=./configuration/<year>/<release>. conf`
and it will run with appropriate configuration. Missing fields will be resolved with `reference.conf`.

The most common configuration changes you will need to make are pointing towards the correct input files. To load files
we use a structure:

```
config-field-name {
      format = "csv"
      path = "path to file"
      options = [
        {k: "sep", v: "\\t"}
        {k: "header", v: true}
      ]
    }
```

The `options` field configures how Spark will read the input files. Both Json and CSV files have a large number of
configurable options, details of which can be
found [in the documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html)

#### Configuring Spark

If you want to use a local installation of Spark customise the `application.conf` with the following spark-uri field and
adjust any other fields as necessary from the `reference.conf` template:

```conf
spark-uri = "local[*]"
common {
 ...  
}
```

By default Spark will only write to non-existent directories. This behaviour can be modified using the settings field
`spark-settings.write-mode` using one of the valid inputs "error", "errorifexists", "append", "overwrite", "ignore".

#### Configuring Loggging

Similarly update the logback configuration. You can add `-Dlogback.configurationFile=application.xml` and have a
logback.xml hanging on your project root or run path. An example log configuration file:

```xml

<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%level %logger{15} - %message%n%xException{10}</pattern>
        </encoder>
    </appender>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>

    <logger name="io.opentargets.etl" level="DEBUG"/>
    <logger name="org.apache.spark" level="WARN"/>

</configuration>
```

and try to run one command as follows

```bash
java -server -Xms1G -Xmx6G -Xss1M -XX:+CMSClassUnloadingEnabled \
    -Dlogback.configurationFile=application.xml \
    -Dconfig.file=./application.conf \
    -classpath . \
    -jar io-opentargets-etl-backend-assembly-0.1.0.jar [step1 [step2 [...]]]
```

### Create a fat JAR

Simply run the following command:

```bash
sbt assembly
```
The jar will be generated under target/scala-2.12.10/

### Create cluster and launch

Here how to create a cluster using `gcloud` tool.


The current image version is `preview` because is the only image that supports `Spark3` and Java11.

List of [dataproc releases]([https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-preview])


```sh
gcloud beta dataproc clusters create \
    etl-cluster \
    --image-version=2.0-debian10 \
    --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-16 \
    --master-boot-disk-size=500 \
    --num-secondary-workers=0 \
    --worker-machine-type=n1-standard-16 \
    --num-workers=2 \
    --worker-boot-disk-size=500 \
    --zone=europe-west1-d \
    --project=open-targets-eu-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=30m
```

And to submit the job (the jar can also by specified from a gs://...

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --files=mk-latest.conf \
           --properties=spark.executor.extraJavaOptions=-Dconfig.file=mk-latest.conf,spark.driver.extraJavaOptions=-Dconfig.file=mk-latest.conf \
           --jar=gs://ot-snapshots/etl/jars/io-opentargets-etl-backend-assembly-0.2.5.jar -- disease
```

where `mk-latest.conf` is

```conf
common {
  output = "gs://ot-snapshots/etl/mk-latest"
}
```

### Using Dataproc workflows

Once [PIS](https://github.com/opentargets/platform-input-support) has provided all of the necessary inputs, update the
configuration files to make use of the new inputs. Create two input files, one using `parquet` as the output format, and
one using `json`. Consult `opentargets-pre-data-releases/22.02.3/` for reference.

If there have been any changes to the ETL create new jars for the literature and ETL project and push them to google
storage.

Run the worksheet `./dataproc/workflow-etl.sc` which will create a cluster, run all steps and then destroy the cluster.

## Step dependencies

Considering only the inputs/outputs of the ETL there are component diagrams available in the 'documentation'
directory. _etl\_current_ shows the relationships between steps in the ETL. _etl\_current\_full_ shows those
relationships at a more granular level, where inputs and outputs are specifically specified.

By default if a step is run that has dependencies the dependent steps will be run first. For example, if you want to
run `target`, `reactome` will be run automatically as `target` depends on `reactome`.

If you only wish to run a single specified step set the field `etl-dag.resolve` to `false`. In this case it is _your
responsibility_ to ensure that required inputs are available.

## Step notes

### Target Validation

Inputs can be provided here where the only logic is to match an ENSG ID against an input column. Any input rows which
can't be matched to a target in the platform will be discarded.

Using mouse phenotypes as an example: we match on the column specified in the configuration `targetFromSourceId`. Any
entry which cannot be matched to an ID in the platform's target dataset will be discarded.

### Evidence

The Evidence step provides scores for evidence strings by datasource. Detailed information can be found in the
[documentation](https://platform-docs.opentargets.org/evidence). Each input file contains the columns "targetId",
"targetFromSourceId", "diseaseId" and "datasourceId", as well as optional extra columns for that data-source.

Scores are calculated using a different formula for each data source configured by the
field `evidences.data-sources. score-expr` (score expression). The score expression is not statically checked for
correctness!

Specific data types can be excluded in one of two ways:

1. Remove the entry from `data-sources`
2. Add the value of `data-sources.id` to the field `data-sources-exclude`. Using this option allows users to exclude
   specific source without having to update the `reference.conf`. For example, to exclude `ot_crispr` and `chembl`,
   update your local configuration with `data-sources-exclude = ["ot_crispr", "chembl"]`.

Output are partitioned by `data-sources.id`.

### Drug

The primary input source of the Drug dataset is ChEMBL. ChEMBL contains almost 2 million molecules, most which are are
not 'drugs'. We define a drug to be any molecule that meets one or more of the following criteria:

- There is at least 1 known indication;
- There is at least 1 known mechanism of action; or
- The ChEMBL ID can be mapped to a DrugBank ID.

#### Inputs

| Input | Source | Notes |
| --- | --- | --- | 
| chembl-molecule | [ChEMBL](https://www.ebi.ac.uk/chembl/) | Provided from PIS using ChEMBL private ES server. |
| chembl-indication | [ChEMBL](https://www.ebi.ac.uk/chembl/) | Provided from PIS using ChEMBL private ES server. |
| chembl-mechanism | [ChEMBL](https://www.ebi.ac.uk/chembl/)| Provided from PIS using ChEMBL private ES server. |
| chembl-target | [ChEMBL](https://www.ebi.ac.uk/chembl/)| Provided from PIS using ChEMBL private ES server. |
| chembl-warning | [ChEMBL](https://www.ebi.ac.uk/chembl/) | Provided from PIS using ChEMBL private ES server. |
| disease-etl | ETL step 'disease' ||
| target-etl | ETL step 'target' ||
| drugbank-to-chembl | [Drugbank vocabulary](https://go.drugbank.com/releases/latest#open-data) | File `Drugbank Vocabulary` |

### Baseline Expression

The primary input sources of the baseline expression dataset are

- The human protein atlas (https://www.proteinatlas.org/)
- Opentarget baseline expressions resources

We define a baseline expression information as

- gene
    - tissues[]
        - label
    - efo_code
    - organs[]
    - anatomical_system[]
    - rna
    - protein
      - level
      - reliability
      - cell_type[]


To run the `baseline expression` step use the example command under `Create a fat JAR` with `expression` as the step name.

### EBI Search datasets
The primary input sources for generating the EBI Search datasets are
 *) Diseases 
 *) Targets 
 *) Evidence
 *) Association Direct Overalls

The step `ebisearch` will generate two datasets with targetId, diseaseId, approvedSymbol, name and score.


#### Adding additional resources to enrich data

Addition resources can be specified to enrich the data included in the outputs. The following `extension-type`s are
 supported: 
 
 - `synonyms`
 - `cross-references`
 
See the sections below for more details on required data structure and limitations. 

Additional resources are specified in the configuration as follows:  
```
      drug-extensions = [
        {
          extension-type = <extension type>
          path = <path to file> 
        }
      ]
    }
```
##### Synonyms

The Drug Beta step supports the addition of supplementary synonym data sources subject to the following limitations:

- The input file(s) must be: 
  - in json format
  - have a field called 'id' which maps 1-to-1 to either a Drugbank ID or ChEMBL ID. The 'id' field must not contain a 
  mixture of both. If the ID is unknown the data will be discarded silently. If a mixture of ids are provided, it is
   indeterminate which of the two will be used. 
  - have a field called 'synonyms' which are either Strings or arrays of Strings linked to the 'id' field. 
  
The input files are specified in the configuration file under the field `drug-extensions`. The files can contain
 additional columns; these will be safely ignored. 
 
New synonyms are added to the 'synonyms' field on the object if they are not already present in either 'synonyms' or
'trade names'. At present it is not possible to add new fields to 'trade names'.

##### Cross references 

The Drug Beta step supports the addition of supplementary cross reference data sources subject to the following
 limitations:

- The input file(s) must: 
  - in json format
  - have a fields: 
    - 'id' which maps 1-to-1 to a ChEMBL ID.
    - 'source'
    - 'reference'  

For example: 

```jsonl
{"id": ..., "source": ..., "reference": ... }
```
  
The input files are specified in the configuration file under the field `drug-extensions`. The files can contain
 additional columns; these will be safely ignored. 
 
If the `source` already exists the new references will be appended to the existing ones, provided that the `reference
` is not already present. If the `source` does not exist it will be created. 
 
#### Inputs

Inputs are specified in the `reference.conf` file and include the following: 

| Name | Source |
| --- | --- |
|   `drug-chembl-molecule` | ChEMBL - Platform Input Support |
|   `drug-chembl-indication` | ChEMBL - Platform Input Support |
|   `drug-chembl-mechanism` | ChEMBL - Platform Input Support |
|   `drug-chembl-target` | ChEMBL - Platform Input Support |
|   `drug-drugbank` | Release annotation file |

The `Drug` step also relies on several other outputs from the ETL:

| Name in Drug | Field in configuration file |
| --- | --- |
| `efo` | disease | 
| `gene` | target | 
| `evidence` | evidence | 

#### Outputs

The `Drug` step writes three files under the common directory specified in the `drug.output.path` configuration field:

- drugs
- mechanismsOfAction
- Indications

Each of these outputs includes a field `id` to allow later linkages between them.

### GO

The 'Go' step generates a small lookup table of gene ontologies.

#### Inputs

| Input | Source | Notes |
| --- | --- | --- |
| go-input | PIS | Provided by PIS from http://geneontology.org/docs/download-ontology/#go_obo_and_owl |

The input is a flat file which does not lend itself to columnar processing so its currently a 'preprocessor' step. If
more complicated logic becomes required this should be ported. There is also to option of querying the EBI API but this
is quite slow and results in a moderately large dataset which we don't otherwise need.

### Target

Generates an index of genes, each uniquely identified by an EnsemblID number. There are approximately 61000 genes
available.

#### Configuration

- `hgnc-orthology-species` lists the species to include in Target orthologues. The order of this configuration list is
  __significant__ as it is used to determine the order in which entries appear in the front-end. Items earlier in the
  list are more closely related to homo sapiens than items further down the list. This is used to select which species
  will be included in Target > Homologues. If you want to add a species to this list you must also update Platform Input
  Support to retrieve that species' gene data.

#### Inputs

Inputs to the ETL are prepared by Platform Input Support (PIS). PIS does some minimal preprocessing, but it is possible
to manually retrieve them and run the step locally. If you would like to run this step locally, retrieve the necessary
inputs from one of the Open Targets public input buckets. eg. `gs://ot-snapshots/...` rather than downloading the files
directly from the sources listed here which are included _for reference_.

Consult the `reference.conf` file to see how to configure the inputs, most of these require only changing the paths to
the data. Options for parsing the inputs should not need to be updated.

1. HGNC
   - `https://storage.googleapis.com/open-targets-data-releases/21.02/input/annotation-files/hgnc_complete_set-2021-02-09.json`

2. Ensembl

    - Use Ensembl human gene JSON file (available
      from: `ftp://ftp.ensembl. org/pub/release-102/json/homo_sapiens/homo_sapiens. json`) updating the release as
      required.
    - It can be useful to convert this file to jsonl format. It can be converted
      with `jq -c . genes[] homo_sapiens. json >> homo_sapiens.jsonl`. The file is 4GB, so needs a decent machine (min
      32GB RAM) for conversion.

3. Uniprot
    - `uniprot`:
        - the Uniprot format in flat txt format instead of xml. This is a flat text file and is provided by PIS. Can be
          downloaded manually
          from `https://www.uniprot. org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=txt`
        - The is a conversion tool to create Scala objects in `io.opentargets.etl.preprocess.uniprot`
    - `uniprot-ssl`: Uniprot subcellular annotation file. Available
      from `https://www.uniprot.org/locations/?query=*&format=tab&force=true&columns=id&compress=yes`
4. Gene Ontology
    - Requires files available from EBI:
        - [Annotation files for human proteins](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz)
        - [Annotation files for human RNAs](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human_rna.gaf.gz)
        - [File for eco lookup](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gpa.gz)
        - [RNAcentral to Ensembl mapping files](ftp://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/database_mappings/ensembl.tsv)

5. Tep
    - Uses files downloaded for `tep` key in PIS's `config.yaml`.
4. NCBI
    - `ncbi`: Used for synonyms, data available
      from: `ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens. gene_info.gz`
6. Human Protein Atlas
    - `hpa` Used for subcellular locations. Data available
      from [HPA's website](https://www.proteinatlas.org/download/subcellular_location.tsv.zip)
    - `hpa-sl-ontology`: Additional file provided by data team to map HPA locations to subcellular location ontology.
7. Project Scores
   - Available from [Cancer Dependency Map](https://score.depmap.sanger.ac.uk/downloads)
   - `ps-gene-identifier`: https://cog.sanger.ac.uk/cmp/download/binaryDepScores.tsv.zip
   - `ps-essentiality-matrix`: https://cog.sanger.ac.uk/cmp/download/essentiality_matrices.zip
8. ChEMBL
    - Target index
9. Gnomad
    - Used for genetic constraints. Data available
      from [Gnomad website](https://storage.googleapis.com/gcp-public-data--gnomad/release/2.1.1/constraint/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz)
    - The file is in `bgz` format, this can be converted to csv with `gunzip -c input > output.csv`.
10. Homologs
    - Update the release number as required:
        - ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.protein_default.homologies.tsv.gz
        - ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.ncrna_default.homologies.tsv.gz
        - ftp://ftp.ensembl.org/pub/release-100/species_EnsemblVertebrates.txt
    - Files generated by PIS: `104_homology_<species>.tsv` where '104' is the Ensembl Release. This is a file of name
      and gene ids to get the correct name for homology gene ids. There will be one for each species.
    - Both the Compara files should be loaded at the same time in `target.input.homology-coding-proteins`
10. Chemical Probes
    - Input file prepared by Data team and made available by PIS.
1. Reactome
    - Raw reactome file from [Reactome downloads](https://reactome.org/download-data) page, using
      the [Ensembl to pathways file](https://reactome.org/download/current/Ensembl2Reactome.txt)
    - Output of ETL step `reactome`
2. Target safety
    - _ToxCast_ data from data team, using latest from [otar-core](gs://otar001-core/TargetSafety/data_files/ToxCast*.
      tsv)
    - _adverseEvents_ and _safetyRisk_ data available from [otar-core](gs://otar001-core/TargetSafety/data_files/).
        - Note: Prior to the 21.09 release this information was stored in a spreadsheet which had not been updated since
          2019. This spreadsheet has been converted into json/parquet files using
          a [utility script](scripts/deprecate_target_safety_spreadsheet.sc). This script should not have to be re-run
          as __the spreadsheet should no longer be used__. If the underlying data changes it should be modified in the
          json/parquet files.
1. Tractability
    - File provided by ChEMBL: https://storage.googleapis.com/otar001-core/Tractability/21.
      08/tractability_buckets_<latest>.tsv

### OpenFDA FAERS DB

The openFDA drug adverse event API returns data that has been collected from the FDA Adverse Event Reporting System (FAERS),
a database that contains information on adverse event and medication error reports submitted to FDA.

#### Data Processing Summary

1. OpenFDA "FAERS" data is collected from [here](https://open.fda.gov/apis/drug/event/download/) (~ 1000 files - May 2020)
2. __Stage 1:__ Pre-processing of this data (OpenFdaEtl.scala):
    - Filtering:
        - Only reports submitted by health professionals (*primarysource.qualification* in (1,2,3)).
        - Exclude reports that resulted in death (no entries with *seriousnessdeath*=1).
        - Only drugs that were considered by the reporter to be the cause of the event (*drugcharacterization*=1).
        - Remove events (but not the whole report which might have multiple events) that are [blacklisted ](https://raw.githubusercontent.com/opentargets/platform-etl-backend/master/src/main/resources/blacklisted_events.txt) (see [Blacklist](#blacklist)).
    - Match FDA drug names to Open Targets drug names & then map all these back to their ChEMBL id:
        - Open Targets drug index fields:  *‘chembl_id’, ‘synonyms’, ‘pref_name’, ‘trade_names’*.
        - openFDA adverse event data fields: *‘drug.medicinalproduct’, ‘drug.openfda.generic_name’, ‘drug.openfda.brand_name’, ‘drug.openfda.substance_name’*.
    - Generate table where each row is a unique drug-event pair and count the number of report IDs for each pair, the total number of reports, the total number of reports per drug and the total number of reports per event. Using these calculate the fields required for estimating the significance of each event occuring for each drug, e.g. log-likelihood ratio, (llr) (based on [FDA LRT method](https://openfda.shinyapps.io/LRTest/_w_c5c2d04d/lrtmethod.pdf)).
3. __Stage 2:__ Calculate significance of each event for all drugs based on the FDA LRT method (Monte Carlo simulation) (MonteCarloSampling.scala).

#### Sample output

```json lines
{"chembl_id":"CHEMBL1231","event":"cardiac output decreased","meddraCode": ..., "count":1,"llr":8.392140045623442,"critval":4.4247991585588675}
{"chembl_id":"CHEMBL1231","event":"cardiovascular insufficiency","meddraCode": ..., "count":1,"llr":7.699049533524681,"critval":4.4247991585588675}
```

Notice that the JSON output is actually JSONL. Each line is a single result object.

#### Configuration

The base configuration is under `src/main/resources/reference.conf`, `openfda` section.

##### CHEMBL Drugs
Refers to the `drug` output this pipeline's drug step.

##### OpenFDA FAERS Data
This section refers to the path where the OpenFDA FAERS DB dump can be found, for processing.

##### Blacklisted Events
Path to the file where to find the list of events that need to be removed from the events in OpenFDA FAERS data, as described above.
This list is manually curated.

##### Meddra (optional)
This pipeline uses an _optional_ subset of data from the [Medical Dictionary for Regulatory Activities](https://www.meddra.org)
to link the adverse event terms used by the FDA back to their standardised names.

This data is included in the pipeline output provided by Open Targets, but if you are running the pipeline locally you
need to download the most recent Meddra release which is subject to licensing restrictions.

In this section, the path to Meddra data release, if available, is specified. Remove key from configuration file if data is not available. As this is proprietary
data it must be provided on an optional basis for users who do not have this dataset.

#### Montecarlo
Specify the number of permutations and the relevance percentile threshhold.

##### Sampling
This subsection configures the sampling output from this ETL step.

##### Outputs
ETL Step outputs.

###### Unfiltered OpenFDA
This is the OpenFDA FAERS data just before running Montecarlo on it, with or without (depending on whether it was provided) Meddra information.

###### OpenFDA Processing Results
This is the result data from this ETL step.

##### Sampling
As the FAERS database is very large (~130GB) it might be useful to extract a stratified sample for analysis and testing
purposes.

> Caution: Execution of the job will be very slow with sampling enabled because of the large amount of
> data which needs to be written to disk!

The sampling method will return a subset of the original data, with equal proportions of drugs which likely would have
had significant results during MC sampling and those that did not. For instance, if there are 6000 unique ChEMBL entries
in the entire dataset, and 500 would have had significant adverse effects, and sampling is set to 0.10, we would expect
that the sampled data would have around 600 ChEMBL entries, of which 50 may be significant. As the sampling is random,
and the significance of an adverse event depends both on the number of adverse events and drugs in a sample, results are
not reproducible. Sampling is provided for basic validation and testing.

The sampled dataset is saved to disk, and can be used as an input for subsequent jobs.

## Development environment notes

### Scalafmt Installation

A pre-commit hook to run [scalafmt](https://scalameta.org/scalafmt/) is recommended for this repo though installation of
scalafmt is left to developers. The [Installation Guide](https://scalameta.org/scalafmt/docs/installation.html)
has simple instructions, and the process used for Ubuntu 18.04 was:

```bash
cd /tmp/  
curl -Lo coursier https://git.io/coursier-cli &&
    chmod +x coursier &&
    ./coursier --help
sudo ./coursier bootstrap org.scalameta:scalafmt-cli_2.12:2.2.1 \
  -r sonatype:snapshots \
  -o /usr/local/bin/scalafmt --standalone --main org.scalafmt.cli.Cli
scalafmt --version # "scalafmt 2.2.1" at TOW
```

The pre-commit hook can then be installed using:

```bash
cd $REPOS/platform-etl-backend
chmod +x hooks/pre-commit.scalafmt 
ln -s $PWD/hooks/pre-commit.scalafmt .git/hooks/pre-commit
```

After this, every commit will trigger scalafmt to run and ```--no-verify``` can be 
used to ignore that step if absolutely necessary.



# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
