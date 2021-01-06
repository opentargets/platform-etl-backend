[![Build Status](https://travis-ci.com/opentargets/platform-etl-backend.svg?branch=master)](https://travis-ci.com/opentargets/platform-etl-backend)

# Open Targets Post-Pipeline ETL process

OpenTargets ETL pipeline to process Pipeline output in order to obtain a new API shaped entities. For the file 
`platformDataBackend.sc`

## platformDataBackend.sc

### Requirements

1. OpenJDK 1.8
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. Drug index dump from OpenTargets ES
5. Target index dump from OpenTargets ES
6. Disease index dump from OpenTargets ES
7. Evidence index dump from OpenTargets ES
8. Expression index dump from OpenTargets ES
9. Generate MousePhenotypes dump from OperTargets ES

### Generate the indices dump from ES7

You will need to either connect to a machine containing the ES or forward the ssh port from it
```sh
elasticdump --input=http://localhost:9200/<indexyouneed> \
    --output=<indexyouneed>.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

Generate MousePhenotypes input file
```sh
cat 20.04_gene-data.json | jq -r '{"id":.id,"phenotypes": [.mouse_phenotypes[]?] }|@json' > mousephenotype.json
```
Copy the file in google storage or specific path

### Load with custom configuration

Add to your run either commandline or sbt task Intellij IDEA `-Dconfig.file=application.conf` and it
will load the configuration from your `./` path or project root. Missing fields will be resolved
with `reference.conf`.

If you want to customise local spark run without any submit in your local machine. Example of
`application.conf`.

```conf
spark-uri = "local[*]"
common {
  output = "etl/latest"
  inputs {
    target {
      format = "parquet"
      path = "luts/gene_parquet/"
    }
    disease  {
      format = "parquet"
      path = "luts/efo_parquet/"
    }
    drug  {
      format = "parquet"
      path = "luts/drug_parquet/"
    }
    evidence  {
      format = "parquet"
      path = "luts/evidence_parquet/"
    }
    associations  {
      format = "parquet"
      path = "luts/association_parquet/"
    }
    ddr  {
      format = "parquet"
      path = "luts/relation_parquet/"
    }
    reactome {
      format = "parquet"
      path = "luts/rea_parquet/"
    }
    eco  {
      format = "parquet"
      path = "luts/eco_parquet/"
    }
    expression {
      format = "parquet"
      path = "luts/expression_parquet/"
    }
    tep {
      format ="json"
      path = "gs://open-targets-data-releases/20.04/input/annotation-files/tep-2020-05-20.json"
    }
    mousephenotypes {
      format ="json"
      path = "gs://ot-snapshots/jsonl/20.04/20.04_mousephenotypes.json"
   }
 }
}

```

The same happens with logback configuration. You can add `-Dlogback.configurationFile=application.xml` and
have a logback.xml hanging on your project root or run path. An example log configuration
file:

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

Here how to create a cluster using `gcloud` tool

```sh
gcloud beta dataproc clusters create \
    etl-cluster \
    --image-version=1.5-debian10 \
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

### Adding ETL outputs to ElasticSearch

The `elasticsearch` directory in the project root folder includes utility scripts to load the outputs of the ETL into
a preconfigured ElasticSearch instance. 

#### Requirements

1. Python utility `elasticsearch_loader` must be installed and on your `PATH`
2. An open port of the ES instance must be forwarded to your local machine and execute the relevant script.

    ```
    gcloud beta compute ssh --zone "europe-west1-d" "es7-20-09" --project "open-targets-eu-dev" -- -L 9200:localhost:9200
    ```  

3. Update the `env.sh` script:
    - `PREFIX` refers to the path to the data to be loaded into Elasticsearch
    - `ES` is the url of Elasticsearch
    - `INDEX_SETTINGS` is the index configuration file. Typically this will be the `index_settings.json` file provided
   in the _elasticsearch_ directory.

4. Export the necessary environment variables by running `source [path to file]env.sh`
4. Run scripts relevant to the index you wish to create, or `load_all.sh` to load all of them.  

## Steps independent of the Data Pipeline

The majority of the ETL was written to process data which has been prepared by the data pipeline for subsequent
 processing. It is intended that this pipeline will be deprecated; because of this some steps do not require inputs
  from the data pipeline to function correctly. These include:
  
  - Drug
  
## Step notes

### Drug

The primary input source of the Drug dataset is ChEMBL. ChEMBL contains almost 2 million molecules, most which are are 
not 'drugs'. We define a drug to be any molecule that meets one or more of the following criteria: 
 - There is at least 1 known indication;
 - There is at least 1 known mechanism of action; or
 - The ChEMBL ID can be mapped to a DrugBank ID.  
 
To run the `Drug` step use the example command under `Create a fat JAR` with `drug` as the step name. 
 
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

## Development environment notes

### Scalafmt Installation

A pre-commit hook to run [scalafmt](https://scalameta.org/scalafmt/) is recommended for 
this repo though installation of scalafmt is left to developers. The [Installation Guide](https://scalameta.org/scalafmt/docs/installation.html)
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
