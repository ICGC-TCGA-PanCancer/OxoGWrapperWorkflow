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
/*		Job manageGit = workflow.createBashJob("git_manage_" + src + "_" + dst);
		String path = JSONlocation + "/" + JSONrepoName + "/" + JSONfolderName;
		// It shouldn't be necessary to do this config again if it was already
		// done in pullRepo, but probably safer this way.
		manageGit.getCommand().addArgument("git config --global user.name " + GITname + " \n");
		manageGit.getCommand().addArgument("git config --global user.email " + GITemail + " \n");
		// I think maybe it should be an error if the *repo* doesn't exist.
		manageGit.getCommand().addArgument("cd " + JSONlocation + "/" + JSONrepoName + " \n");
		manageGit.getCommand().addArgument("[ -d " + path + " ] || mkdir -p " + path + " \n");
		manageGit.getCommand().addArgument("cd " + path + " \n");
		manageGit.getCommand().addArgument("# This is not idempotent: git pull \n");

		// If gitMoveTestMode is true, then the file moves will only happen
		// locally, but will not be checked into git.
		//TODO: Add a retry mechanism here. Git operations may fail but that doesn't mean the whole workflow should fail immediately. Retrying should be possible.
		if (!gitMoveTestMode) {
			manageGit.getCommand().addArgument("git checkout master \n");
			manageGit.getCommand().addArgument("git reset --hard origin/master \n");
			manageGit.getCommand().addArgument("git pull \n");
		}
		manageGit.getCommand().addArgument("[ -d " + dst + " ] || mkdir -p " + dst + " \n");

		if (!gitMoveTestMode) {
			manageGit.getCommand().addArgument("if [[ -d " + src + " ]]; then git mv " + path + "/" + src + "/"
					+ JSONfileName + " " + path + "/" + dst + "; fi \n");
			manageGit.getCommand().addArgument("git stage . \n");
			manageGit.getCommand().addArgument("git commit -m '" + dst + ": " + JSONfileName + "' \n");
			manageGit.getCommand().addArgument("git push \n");
		} else {
			manageGit.getCommand().addArgument("if [[ -d " + src + " ]]; then mv " + path + "/" + src + "/"
					+ JSONfileName + " " + path + "/" + dst + "; fi \n");
		}

*/
		Job gitMove = workflow.createBashJob("git_move_from_"+src+"_to_"+dst);
		
//		gitMove.setCommand("python "+workflow.getWorkflowBundleDir()+"/scripts/git_move.py ");
//		gitMove.getCommand().addArgument(JSONlocation + "/" + JSONrepoName + "/" + JSONfolderName );
//		gitMove.getCommand().addArgument(src);
//		gitMove.getCommand().addArgument(dst);
//		gitMove.getCommand().addArgument(JSONfileName);
//		//Ensure that string going to python script is formatted properly.
//		String testModeStr = String.valueOf(gitMoveTestMode);
//		gitMove.getCommand().addArgument( testModeStr.substring(0,1).toUpperCase() + testModeStr.substring(1) );
		
		gitMove.setCommand(gitMoveCommand(src, dst, JSONlocation + "/" + JSONrepoName + "/" + JSONfolderName, JSONfileName, gitMoveTestMode,pathToScripts ));
		for (Job p : parents) {
			gitMove.addParent(p);
		}
		return gitMove;
	}
	
	static String gitMoveCommand(String src, String dst, String pathToRootDir, String filename, boolean testMode, String pathToScriptDir)
	{
		String testModeStr = (String.valueOf(testMode)).substring(0,1).toUpperCase() + (String.valueOf(testMode)).substring(1);
		String cmd = "python "+pathToScriptDir + "/git_move.py" + " "+ pathToRootDir + " "+src + " "+dst + " "+filename + " " + testModeStr;
		return cmd;
	}
}
