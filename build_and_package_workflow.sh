#! /bin/bash

mvn clean package
seqware bundle package --dir target/Workflow_Bundle_OxoGWrapper_*_SeqWare_1.1.2

