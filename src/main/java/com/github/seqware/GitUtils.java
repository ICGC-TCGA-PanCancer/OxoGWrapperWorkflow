package com.github.seqware;

import net.sourceforge.seqware.pipeline.workflowV2.model.Job;
import net.sourceforge.seqware.pipeline.workflowV2.model.Workflow;

class GitUtils {
	static Job gitConfig(Workflow workflow, String GitName, String GitEmail)
	{
		Job configJob = workflow.createBashJob("set git name and email");
		
		configJob.getCommand().addArgument("git config --global user.name " + GitName + " ; \n");
		configJob.getCommand().addArgument("git config --global user.email " + GitEmail + " ; \n");
		configJob.getCommand().addArgument("git config --list  ; \n");
		return configJob;
	}
	
	/**
	 * Pulls files from a git repository.
	 * 
	 * @param GITname
	 * @param JSONrepo
	 * @param JSONrepoName
	 * @param JSONlocation
	 * @param GITemail
	 * @return
	 */
	static Job pullRepo(Workflow workflow, String GITPemFile,  String JSONrepo, String JSONrepoName,
			String JSONlocation) {
				
		Job pullRepoJob = workflow.createBashJob("pull_git_repo");
		pullRepoJob.getCommand().addArgument("if [[ ! -d ~/.ssh/ ]]; then  mkdir ~/.ssh; fi \n");
		pullRepoJob.getCommand().addArgument("cp " + GITPemFile + " ~/.ssh/id_rsa \n");
		pullRepoJob.getCommand().addArgument("chmod 600 ~/.ssh/id_rsa \n");
		pullRepoJob.getCommand().addArgument("echo 'StrictHostKeyChecking no' > ~/.ssh/config \n");
		pullRepoJob.getCommand().addArgument("[ -d " + JSONlocation + " ] || mkdir -p " + JSONlocation + " \n");
		pullRepoJob.getCommand().addArgument("cd " + JSONlocation + " \n");
		pullRepoJob.getCommand()
				.addArgument("[ -d " + JSONlocation + "/" + JSONrepoName + " ] || git clone " + JSONrepo + " \n");
		// pullRepoJob.getCommand().addArgument("echo $? \n");

		pullRepoJob.getCommand().addArgument("echo \"contents: \"\n");
		pullRepoJob.getCommand().addArgument("ls -lRA  \n");
		return pullRepoJob;
	}

	/**
	 * * Moves file "this.JSONfileName" from one local directory in a git repo
	 * to another directory, and then commits the change
	 * 
	 * @param src
	 *            The source directory
	 * @param dst
	 *            The destination directory
	 * @param workflow
	 * @param JSONlocation
	 * @param JSONrepoName
	 * @param JSONfolderName
	 * @param GITname
	 * @param GITemail
	 * @param gitMoveTestMode
	 * @param JSONfileName
	 * @param parents
	 *            A list of parent jobs for this job.
	 * @return
	 * @throws Exception
	 */
	static Job gitMove(String src, String dst, Workflow workflow, String JSONlocation, String JSONrepoName,
			String JSONfolderName, String GITname, String GITemail, boolean gitMoveTestMode, String JSONfileName, String pathToScripts,
			Job... parents) throws Exception {
		if (parents == null || parents.length == 0) {
			throw new Exception("You must provide at least one parent job!");
		}

		Job gitMove = workflow.createBashJob("git_move_from_"+src+"_to_"+dst);
		
		
		gitMove.setCommand(gitMoveCommand(src, dst, JSONlocation + "/" + JSONrepoName + "/" + JSONfolderName, JSONfileName, gitMoveTestMode,pathToScripts));
		for (Job p : parents) {
			gitMove.addParent(p);
		}
		return gitMove;
	}
	
	static String gitMoveCommand(String src, String dst, String pathToRootDir, String filename, boolean testMode, String pathToScriptDir)
	{
		String getIPCommand = "$(ifconfig eth0 | grep \"inet addr\" | sed 's/.*inet addr:\\(.*\\) Bcast:.*/\\1/')";
		String testModeStr = (String.valueOf(testMode)).substring(0,1).toUpperCase() + (String.valueOf(testMode)).substring(1);
		String cmd = "python "+pathToScriptDir + "/git_move.py" + " "+ pathToRootDir + " "+src + " "+dst + " "+filename + " " + testModeStr + " " + getIPCommand;
		return cmd;
	}
}
