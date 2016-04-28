FROM pancancer/seqware_whitestar_pancancer:1.1.2-actual-java8
MAINTAINER Solomon Shorser <solomon.shorser@oicr.on.ca>

ENV OXOG_WRAPPER_IMAGE_VERSION 2.0.3
LABEL OXOG_WRAPPER_IMAGE_VERSION $OXOG_WRAPPER_IMAGE_VERSION
# For the storage client.
ENV STORAGE_PROFILE=collab

# OxoG Workflow needs tabix, bgzip and samtools
RUN sudo apt-get update && \
	sudo apt-get install -y tabix samtools libstring-random-perl 

USER root

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
RUN mkdir /workflows && \
	ln -s $(pwd)/target/Workflow_Bundle_OxoGWrapper_${OXOG_WRAPPER_IMAGE_VERSION}_SeqWare_1.1.2/Workflow_Bundle_OxoGWrapper \
		/workflows/Workflow_Bundle_OxoGWrapper
USER seqware

# Maybe make an entry point to run the workflow?