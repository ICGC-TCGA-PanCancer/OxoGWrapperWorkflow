package com.github.seqware.jobgenerators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.seqware.CommonPredicates;
import com.github.seqware.GitUtils;
import com.github.seqware.TumourInfo;
import com.github.seqware.VcfInfo;
import com.github.seqware.OxoGWrapperWorkflow.BAMType;
import com.github.seqware.OxoGWrapperWorkflow.DownloadMethod;
import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.downloaders.DownloaderBuilder;
import com.github.seqware.downloaders.GNOSDownloader;
import com.github.seqware.downloaders.ICGCStorageDownloader;
import com.github.seqware.downloaders.S3Downloader;

import net.sourceforge.seqware.pipeline.workflowV2.AbstractWorkflowDataModel;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;

public class DownloadJobGenerator extends JobGeneratorBase {

	private String downloadMethod;
	private String storageSource;
	private String gtDownloadBamKey;
	private String gtDownloadVcfKey;
	private String broadGNOSRepoURL;
	private String sangerGNOSRepoURL;
	private String dkfzEmblGNOSRepoURL;
	private String museGNOSRepoURL;
	private String normalBamGNOSRepoURL;
	private String bamNormalIndexObjectID;
	private String bamNormalObjectID;
	private String GITname;
	private String GITemail;
	private List<TumourInfo> tumours;
	private List<VcfInfo> vcfs;
	private Map<String,String> workflowNamestoGnosIds;
	private Map<String,String> objectToFilenames;

	private String getFileCommandString(DownloadMethod downloadMethod, String outDir, String downloadType, String storageSource, String downloadKey, String ... objectIDs  )
	{
		switch (downloadMethod)
		{
			case icgcStorageClient:
				return ( DownloaderBuilder.of(ICGCStorageDownloader::new).with(ICGCStorageDownloader::setStorageSource, storageSource).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case gtdownload:
				return ( DownloaderBuilder.of(GNOSDownloader::new).with(GNOSDownloader::setDownloadKey, downloadKey).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			case s3:
				return ( DownloaderBuilder.of(S3Downloader::new).build() ).getDownloadCommandString(outDir, downloadType, objectIDs);
			default:
				throw new RuntimeException("Unknown downloadMethod: "+downloadMethod.toString());
		}
	}

	
	/**
	 * Download a BAM file.
	 * @param parentJob
	 * @param objectID - the object ID of the BAM file
	 * @param bamType - is it normal or tumour? This used to determine the name of the directory that the file ends up in.
	 * @return
	 */
	private Job getBAM(AbstractWorkflowDataModel workflow, Job parentJob, DownloadMethod downloadMethod, BAMType bamType, String ... objectIDs) {
		Job getBamFileJob = workflow.getWorkflow().createBashJob("get "+bamType.toString()+" BAM file");
		getBamFileJob.addParent(parentJob);
		
		String outDir = "/datastore/bam/"+bamType.toString()+"/";
		String getBamCommandString;
		getBamCommandString = getFileCommandString(downloadMethod, outDir, bamType.toString(), this.storageSource, this.gtDownloadBamKey, objectIDs);
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		getBamFileJob.setCommand("( "+getBamCommandString+" ) || "+moveToFailed);

		return getBamFileJob;
	}

	private Job getBAM(AbstractWorkflowDataModel workflow,Job parentJob, DownloadMethod downloadMethod, BAMType bamType, List<String> objectIDs) {
		return getBAM(workflow, parentJob, downloadMethod, bamType, objectIDs.toArray(new String[objectIDs.size()]));
	}

	/**
	 This will download VCFs for a workflow, based on an object ID(s).
	 It will perform these operations:
	 <ol>
	 <li>download VCFs</li>
	 <li>normalize INDEL VCF</li>
	 <li>extract SNVs from INDEL into a separate VCF</li>
	 </ol>
	 * @param parentJob
	 * @param workflowName The pipeline (AKA workflow) that the VCFs come from. This will determine the name of the output directory where the downloaded files will be stored.
	 * @param objectID
	 * @return
	 */
	private Job getVCF(AbstractWorkflowDataModel workflow, Job parentJob, DownloadMethod downloadMethod, Pipeline workflowName, String ... objectIDs) {
		//System.out.printf("DEBUG: getVCF: "+downloadMethod + " ; "+ workflowName + " ; %s\n", objectIDs);
		if (objectIDs == null || objectIDs.length == 0)
		{
			throw new RuntimeException("Cannot have empty objectIDs!");
		}
		
		Job getVCFJob = workflow.getWorkflow().createBashJob("get VCF for workflow " + workflowName);
		String outDir = "/datastore/vcf/"+workflowName;
		String moveToFailed = GitUtils.gitMoveCommand("downloading-jobs","failed-jobs",this.JSONlocation + "/" + this.JSONrepoName + "/" + this.JSONfolderName,this.JSONfileName, this.gitMoveTestMode, workflow.getWorkflowBaseDir() + "/scripts/");
		String getVCFCommand ;
		getVCFCommand = getFileCommandString(downloadMethod, outDir, workflowName.toString(), this.storageSource, this.gtDownloadVcfKey, objectIDs);
		getVCFCommand += (" || " + moveToFailed);
		getVCFJob.setCommand(getVCFCommand);
		
		getVCFJob.addParent(parentJob);

		return getVCFJob;
	}

	private Job getVCF(AbstractWorkflowDataModel workflow, Job parentJob, DownloadMethod downloadMethod, Pipeline workflowName, List<String> objectIDs) {
		return this.getVCF(workflow, parentJob, downloadMethod, workflowName, objectIDs.toArray(new String[objectIDs.size()]));
	}
	
	
	public Job doDownload(AbstractWorkflowDataModel workflow, String pathToScripts, Job move2download) throws Exception {
		Job move2running;
		//Download jobs. VCFs downloading serial. Trying to download all in parallel seems to put too great a strain on the system 
		//since the icgc-storage-client can make full use of all cores on a multi-core system. 
		DownloadMethod downloadMethod = DownloadMethod.valueOf(this.downloadMethod);
		
		Function<Predicate<VcfInfo>, List<String>> buildVcfListByPredicate = (p) -> Stream.concat(
				this.vcfs.stream().filter(p).map(m -> m.getObjectID()), 
				this.vcfs.stream().filter(p).map(m -> m.getIndexObjectID())
			).collect(Collectors.toList()) ; 
		
		List<String> sangerList =  buildVcfListByPredicate.apply(CommonPredicates.isSanger);
		List<String> broadList = buildVcfListByPredicate.apply(CommonPredicates.isBroad);
		List<String> dkfzEmblList = buildVcfListByPredicate.apply(CommonPredicates.isDkfzEmbl);
		List<String> museList = buildVcfListByPredicate.apply(CommonPredicates.isMuse);
		List<String> normalList = Arrays.asList( this.bamNormalIndexObjectID,this.bamNormalObjectID);
		//System.out.println("DEBUG: sangerList: "+sangerList.toString());
		Map<String,List<String>> workflowObjectIDs = new HashMap<String,List<String>>(6);
		workflowObjectIDs.put(Pipeline.broad.toString(), broadList);
		workflowObjectIDs.put(Pipeline.sanger.toString(), sangerList);
		workflowObjectIDs.put(Pipeline.dkfz_embl.toString(), dkfzEmblList);
		workflowObjectIDs.put(Pipeline.muse.toString(), museList);
		workflowObjectIDs.put(BAMType.normal.toString(), normalList);
		for (int i = 0; i < this.tumours.size(); i ++)
		{
			TumourInfo tInfo = this.tumours.get(i);
			List<String> tumourIDs = new ArrayList<String>();
			tumourIDs.add(tInfo.getBamTumourIndexObjectID());
			tumourIDs.add(tInfo.getBamTumourObjectID());
			workflowObjectIDs.put(BAMType.tumour+"_"+tInfo.getAliquotID(), tumourIDs);
		}
		
		Map<String,String> workflowURLs = new HashMap<String,String>(6);
		workflowURLs.put(Pipeline.broad.toString(), this.broadGNOSRepoURL);
		workflowURLs.put(Pipeline.sanger.toString(), this.sangerGNOSRepoURL);
		workflowURLs.put(Pipeline.dkfz_embl.toString(), this.dkfzEmblGNOSRepoURL);
		workflowURLs.put(Pipeline.muse.toString(), this.museGNOSRepoURL);
		workflowURLs.put(BAMType.normal.toString(), this.normalBamGNOSRepoURL);
		for (int i = 0 ; i < this.tumours.size(); i++)
		{
			workflowURLs.put(BAMType.tumour.toString()+"_"+tumours.get(i).getAliquotID(), tumours.get(i).getTumourBamGNOSRepoURL());
		}
		
		Function<String,List<String>> chooseObjects = (s) -> 
		{
			switch (downloadMethod)
			{
				case icgcStorageClient:
					// ICGC storage client - get list of object IDs, use s (workflowname) as lookup.
					return workflowObjectIDs.get(s);
				case s3:
					//For S3 downloader, it will take a list of strings. The strings are of the pattern: <object_id>:<file_name> and it will download all object IDs to the paired filename.
					//We prepend the GNOS ID to the filename because other processes have an expectation (from icgc-storage-client and gtdownload) the files will be in a 
					//directory named with the GNOS ID.
					List<String> objects = workflowObjectIDs.get(s);
					List<String> s3Mappings = objects.stream().map(t -> t + ":" + this.workflowNamestoGnosIds.get(s) + "/" + this.objectToFilenames.get(t) ).collect(Collectors.toList());
					return s3Mappings;
				case gtdownload:
					// gtdownloader - look up the GNOS URL, return as list with single item. 
					return Arrays.asList(workflowURLs.get(s));
				default:
					throw new RuntimeException("Unknown download method: "+downloadMethod);
			}
		};
		
		Job downloadSangerVCFs = this.getVCF(workflow, move2download, downloadMethod, Pipeline.sanger, chooseObjects.apply( Pipeline.sanger.toString() ) );
		Job downloadDkfzEmblVCFs = this.getVCF(workflow, downloadSangerVCFs, downloadMethod, Pipeline.dkfz_embl, chooseObjects.apply( Pipeline.dkfz_embl.toString() ) );
		Job downloadBroadVCFs = this.getVCF(workflow, downloadDkfzEmblVCFs, downloadMethod, Pipeline.broad, chooseObjects.apply( Pipeline.broad.toString() ) );
		Job downloadMuseVCFs = this.getVCF(workflow, downloadBroadVCFs, downloadMethod, Pipeline.muse, chooseObjects.apply( Pipeline.muse.toString() ) );
		// Once VCFs are downloaded, download the BAMs.
		Job downloadNormalBam = this.getBAM(workflow, downloadMuseVCFs, downloadMethod, BAMType.normal, chooseObjects.apply( BAMType.normal.toString() ) );
		
		//create a list of jobs to download all tumours.
		List<Job> getTumourJobs = new ArrayList<Job>(this.tumours.size());
		System.out.println("Tumours : "+this.tumours);
		for (int i = 0 ; i < this.tumours.size(); i++)
		{	
			Job downloadTumourBam;
			//download the tumours sequentially.
			Job downloadTumourBamParent;
			if (i==0)
			{
				downloadTumourBamParent = downloadNormalBam;
			}
			else
			{
				downloadTumourBamParent = getTumourJobs.get(i-1);
			}
			downloadTumourBam = this.getBAM(workflow, downloadTumourBamParent, downloadMethod, BAMType.tumour,chooseObjects.apply( BAMType.tumour.toString()+"_"+this.tumours.get(i).getAliquotID() ) );
			getTumourJobs.add(downloadTumourBam);
		}
		
		// After we've downloaded all VCFs on a per-workflow basis, we also need to do a vcfcombine 
		// on the *types* of VCFs, for the minibam generator. The per-workflow combined VCFs will
		// be used by the OxoG filter. These three can be done in parallel because they all require the same inputs, 
		// but none require the inputs of the other and they are not very intense jobs.
		// indicate job is running.
		move2running = GitUtils.gitMove( "downloading-jobs", "running-jobs", workflow.getWorkflow(),
				this.JSONlocation, this.JSONrepoName, this.JSONfolderName, this.GITname, this.GITemail, this.gitMoveTestMode, this.JSONfileName, pathToScripts
				, downloadSangerVCFs, downloadDkfzEmblVCFs, downloadBroadVCFs, downloadMuseVCFs, downloadNormalBam, getTumourJobs.get(getTumourJobs.size()-1));
		return move2running;
	}

	public String getDownloadMethod() {
		return this.downloadMethod;
	}


	public void setDownloadMethod(String downloadMethod) {
		this.downloadMethod = downloadMethod;
	}


	public String getStorageSource() {
		return this.storageSource;
	}


	public void setStorageSource(String storageSource) {
		this.storageSource = storageSource;
	}


	public String getGtDownloadBamKey() {
		return this.gtDownloadBamKey;
	}


	public void setGtDownloadBamKey(String gtDownloadBamKey) {
		this.gtDownloadBamKey = gtDownloadBamKey;
	}


	public String getGtDownloadVcfKey() {
		return this.gtDownloadVcfKey;
	}


	public void setGtDownloadVcfKey(String gtDownloadVcfKey) {
		this.gtDownloadVcfKey = gtDownloadVcfKey;
	}


	public String getBroadGNOSRepoURL() {
		return this.broadGNOSRepoURL;
	}


	public void setBroadGNOSRepoURL(String broadGNOSRepoURL) {
		this.broadGNOSRepoURL = broadGNOSRepoURL;
	}


	public String getSangerGNOSRepoURL() {
		return this.sangerGNOSRepoURL;
	}


	public void setSangerGNOSRepoURL(String sangerGNOSRepoURL) {
		this.sangerGNOSRepoURL = sangerGNOSRepoURL;
	}


	public String getDkfzEmblGNOSRepoURL() {
		return this.dkfzEmblGNOSRepoURL;
	}


	public void setDkfzEmblGNOSRepoURL(String dkfzEmblGNOSRepoURL) {
		this.dkfzEmblGNOSRepoURL = dkfzEmblGNOSRepoURL;
	}


	public String getMuseGNOSRepoURL() {
		return this.museGNOSRepoURL;
	}


	public void setMuseGNOSRepoURL(String museGNOSRepoURL) {
		this.museGNOSRepoURL = museGNOSRepoURL;
	}


	public String getNormalBamGNOSRepoURL() {
		return this.normalBamGNOSRepoURL;
	}


	public void setNormalBamGNOSRepoURL(String normalBamGNOSRepoURL) {
		this.normalBamGNOSRepoURL = normalBamGNOSRepoURL;
	}


	public String getBamNormalIndexObjectID() {
		return this.bamNormalIndexObjectID;
	}


	public void setBamNormalIndexObjectID(String bamNormalIndexObjectID) {
		this.bamNormalIndexObjectID = bamNormalIndexObjectID;
	}


	public String getBamNormalObjectID() {
		return this.bamNormalObjectID;
	}


	public void setBamNormalObjectID(String bamNormalObjectID) {
		this.bamNormalObjectID = bamNormalObjectID;
	}


	public String getGITname() {
		return this.GITname;
	}


	public void setGITname(String gITname) {
		this.GITname = gITname;
	}


	public String getGITemail() {
		return this.GITemail;
	}


	public void setGITemail(String gITemail) {
		this.GITemail = gITemail;
	}


	public List<TumourInfo> getTumours() {
		return this.tumours;
	}


	public void setTumours(List<TumourInfo> tumours) {
		this.tumours = tumours;
	}


	public List<VcfInfo> getVcfs() {
		return this.vcfs;
	}


	public void setVcfs(List<VcfInfo> vcfs) {
		this.vcfs = vcfs;
	}


	public Map<String, String> getWorkflowNamestoGnosIds() {
		return this.workflowNamestoGnosIds;
	}


	public void setWorkflowNamestoGnosIds(Map<String, String> workflowNamestoGnosIds) {
		this.workflowNamestoGnosIds = workflowNamestoGnosIds;
	}


	public Map<String, String> getObjectToFilenames() {
		return this.objectToFilenames;
	}


	public void setObjectToFilenames(Map<String, String> objectToFilenames) {
		this.objectToFilenames = objectToFilenames;
	}
}
