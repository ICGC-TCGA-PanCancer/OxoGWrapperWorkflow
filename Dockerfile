FROM pancancer/seqware_whitestar_pancancer:1.1.1
MAINTAINER Solomon Shorser <solomon.shorser@oicr.on.ca>

#OxoG will need the ICGC Storage Client tool.
RUN mkdir /home/seqware/downloads
WORKDIR /home/seqware/downloads
RUN wget -O icgc-storage-client.tar.gz https://dcc.icgc.org/api/v1/ui/software/icgc-storage-client/latest
RUN tar -xvzf icgc-storage-client.tar.gz
USER root
RUN ln -s ./bin/icgc-storage-client /usr/bin/icgc-storage-client
USER seqware

#Next, Add the workflow and compile.
WORKDIR /home/seqware
RUN mkdir /home/seqware/OxoGWrapperWorkflowCode
COPY ./src					/home/seqware/OxoGWrapperWorkflowCode/src
COPY ./links				/home/seqware/OxoGWrapperWorkflowCode/links
COPY ./pom.xml				/home/seqware/OxoGWrapperWorkflowCode/pom.xml
COPY ./workflow				/home/seqware/OxoGWrapperWorkflowCode/workflow
COPY ./workflow.properties	/home/seqware/OxoGWrapperWorkflowCode/workflow.properties
WORKDIR /home/seqware/OxoGWrapperWorkflowCode/
RUN mvn clean compile package install

#Link the newly built workflow into /workflows
USER root
RUN mkdir /workflows
RUN ln -s $(pwd)/target/Workflow_Bundle_OxoGWrapper_1.0_SeqWare_1.1.2/Workflow_Bundle_OxoGWrapper  /workflows/Workflow_Bundle_OxoGWrapper
USER seqware 