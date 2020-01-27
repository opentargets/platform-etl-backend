## platformDataBackendAssociations.sc

### Aim

This script is a recipe to compute standard _off-line_ associations from
the validated evidences

1. Compute unique direct Target - Disease (T-D) pairs from valid evidences
2. Compute unique indirect T-D pairs based on EFO3 ontology
3. Generate a score per pair using Open Targets Harmonic equation 
and the weights included on the configuration file
4. Compute aggregated fields and insert them into the field `private`

### Outcome

The output of this script is a folder with a partitioned dataset of all 
unique pairs in [JSON Lines](http://jsonlines.org/) format.  

### Requirements

1. OpenJDK 1.8
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. Some dumped indices from ES7 latest release

### Run the scala script

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G> \
    -Dconfig.file=application.conf \
    -Dlogback.configurationFile=logback.xml"

amm platformDataBackendAssociations.sc \
    --expressionFilename luts/19.11_expression-data.json \
    --targetFilename luts/19.11_gene-data.json \
    --diseaseFilename luts/19.11_efo-data.json \
    --evidenceFilename luts/19.11_evidence-data.json \
    --outputPathPrefix out/
```

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
