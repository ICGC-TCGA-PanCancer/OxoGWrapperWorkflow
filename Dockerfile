FROM pancancer/seqware_whitestar_pancancer:1.1.2-actual-java8
MAINTAINER Solomon Shorser <solomon.shorser@oicr.on.ca>

ENV OXOG_WRAPPER_IMAGE_VERSION 1.0.0
LABEL OXOG_WRAPPER_IMAGE_VERSION $OXOG_WRAPPER_IMAGE_VERSION
# For the storage client.
ENV STORAGE_PROFILE=collab

# OxoG Workflow may need tabix, bgzip and samtools
RUN apt-get update && \
	apt-get install -y tabix samtools 

#ICGC Storage Client needs Java 8
#USER root
#RUN apt-get update && \
#	apt-get install software-properties-common python-software-properties  -y
#RUN apt-add-repository 'ppa:webupd8team/java'
#RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
#RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 seen true | /usr/bin/debconf-set-selections
#RUN apt-get update && \
#	apt-get install oracle-java8-installer -y
#ENV JAVA_HOME=/usr/lib/jvm/java-8-oracle
#RUN rm /usr/bin/java && ln -s $JAVA_HOME/jre/bin/java /usr/bin/java

#OxoG will need the ICGC Storage Client tool.
RUN mkdir /home/seqware/downloads
WORKDIR /home/seqware/downloads
RUN wget -O icgc-storage-client.tar.gz https://dcc.icgc.org/api/v1/ui/software/icgc-storage-client/latest && \
	tar -xvzf icgc-storage-client.tar.gz
USER root
#RUN ln -s $(pwd)/icgc-storage-client-*/bin/icgc-storage-client /usr/bin/icgc-storage-client
#icgc-storage-client is actually a shell script that does some weird stuff with relative paths, so we can't link it into anything on $PATH.
#But we *can* make an alias to it.
COPY ./bash_aliases /home/seqware/.bash_aliases
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
RUN mkdir /workflows && \
	ln -s $(pwd)/target/Workflow_Bundle_OxoGWrapper_${OXOG_WRAPPER_IMAGE_VERSION}_SeqWare_1.1.2/Workflow_Bundle_OxoGWrapper  /workflows/Workflow_Bundle_OxoGWrapper
USER seqware

# Maybe make an entry point to run the workflow?