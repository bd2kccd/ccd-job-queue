/*
 * Copyright (C) 2015 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.ccd.queue.service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import edu.pitt.dbmi.ccd.connection.SlurmClient;
import edu.pitt.dbmi.ccd.connection.slurm.JobStat;
import edu.pitt.dbmi.ccd.connection.slurm.JobStatus;
import edu.pitt.dbmi.ccd.db.entity.JobQueueInfo;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.JobQueueInfoService;

/**
 * 
 * May 24, 2016 6:08:41 PM
 * 
 * @author Chirayu (Kong) Wongchokprasitti
 *
 */
@Profile("slurm")
@Service
@EnableAsync
public class AlgorithmSlurmService {

	private static final Logger LOGGER = LoggerFactory.getLogger(AlgorithmSlurmService.class);

	private final JobQueueInfoService jobQueueInfoService;

	private final SlurmClient client;

	private final String workspace;

	private final String dataFolder;

	private final String tempFolder;
	
	private final String resultFolder;

	private final String algorithmResultFolder;

	private final String jobTemplates;

	private final String checkUserDir;
	
	private final String causalJob;

	private final String remotedataspace;
	
	private final String remoteworkspace;
	
	private final String checkUserDirScript;
	
	private final String runSlurmJobScript;

	@Autowired(required = true)
	public AlgorithmSlurmService(@Value("${ccd.server.workspace}") String workspace,
			@Value("${ccd.folder.data:data}") String dataFolder,
			@Value("${ccd.folder.tmp:tmp}") String tempFolder,
			@Value("${ccd.folder.results:results}") String resultFolder,
			@Value("${ccd.folder.results.algorithm:algorithm}") String algorithmResultFolder,
			@Value("${ccd.folder.job_templates}") String jobTemplates, 
			@Value("${ccd.template.checkuserdir}") String checkUserDir,
			@Value("${ccd.template.causaljob}") String causalJob,
			@Value("${ccd.remote.server.dataspace}") String remotedataspace,
			@Value("${ccd.remote.server.workspace}") String remoteworkspace,
			@Value("${ccd.script.checkuserdir:checkUserDir.sh}") String checkUserDirScript,
			@Value("${ccd.script.runslurmjob:runSlurmJobScript.sh}") String runSlurmJobScript,
			JobQueueInfoService queuedJobInfoService) {
		this.jobQueueInfoService = queuedJobInfoService;
		this.client = new SlurmClient();
		this.workspace = workspace;
		this.dataFolder = dataFolder;
		this.tempFolder = tempFolder;
		this.resultFolder = resultFolder;
		this.algorithmResultFolder = algorithmResultFolder;
		this.jobTemplates = jobTemplates;
		this.checkUserDir = checkUserDir;
		this.causalJob = causalJob;
		this.remotedataspace = remotedataspace;
		this.remoteworkspace = remoteworkspace;
		this.checkUserDirScript = checkUserDirScript;
		this.runSlurmJobScript = runSlurmJobScript;
	}
	
	public JobStat getJobStat(Long jobId){
		JobStat stat = null;
		try {
			stat = client.getJobStat(jobId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stat;
	}

	public List<JobStatus> getFinishedJobs() {
		List<JobStatus> jobs = null;
		try {
			jobs = client.getFinishedJobs();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobs;
	}
	
	private void deleteRunSlurmScript(JobQueueInfo jobQueueInfo){
		Long queueId = jobQueueInfo.getId();
		Set<UserAccount> userAccounts = jobQueueInfo.getUserAccounts();
		UserAccount userAccount = (UserAccount) userAccounts.toArray()[0];
		String username = userAccount.getUsername();
		
		Path scriptPath = Paths.get(remoteworkspace, username, runSlurmJobScript);
		String scriptDir = scriptPath.toAbsolutePath().toString() + queueId  + ".sh";
		if(client.remoteFileExisted(scriptDir)){
			client.deleteRemoteFile(scriptDir);
		}
	}
	
	@Async
	public Future<Void> cancelSlurmJob(JobQueueInfo jobQueueInfo){
		Long jobId = jobQueueInfo.getPid();
		
		try {
			client.cancelJob(jobId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		deleteRunSlurmScript(jobQueueInfo);
		
		return new AsyncResult<>(null);
	}
	
	//@Async
	public Future<Void> downloadJobResult(JobQueueInfo jobQueueInfo) {
		String fileName = jobQueueInfo.getFileName() + ".txt";
		String tmpDirectory = jobQueueInfo.getTmpDirectory();
		String outputDirectory = jobQueueInfo.getOutputDirectory();

		Path src = Paths.get(tmpDirectory, fileName);
		Path dest = Paths.get(outputDirectory, fileName);

		String errorFileName = String.format("error_%s", fileName);
		Path error = Paths.get(tmpDirectory, errorFileName);
		Path errorDest = Paths.get(outputDirectory, errorFileName);

		try {
			if(client.remoteFileExisted(src.toAbsolutePath().toString())){
				client.downloadOutput(src.toAbsolutePath().toString(), dest.toAbsolutePath().toString());
				client.deleteRemoteFile(src.toAbsolutePath().toString());
				
			}else{
				client.downloadOutput(error.toAbsolutePath().toString(), errorDest.toAbsolutePath().toString());
			}
			client.deleteRemoteFile(error.toAbsolutePath().toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		deleteRunSlurmScript(jobQueueInfo);
		
		return new AsyncResult<>(null);
	}
	
	//@Async
	public Future<Void> submitJobtoSlurm(JobQueueInfo jobQueueInfo) {
		Long queueId = jobQueueInfo.getId();
		String fileName = jobQueueInfo.getFileName() + ".txt";
		String commands = jobQueueInfo.getCommands();
		String tmpDirectory = jobQueueInfo.getTmpDirectory();

		Properties p = new Properties();
		
		// Upload dataset(s) to the remote data storage
		Set<UserAccount> userAccounts = jobQueueInfo.getUserAccounts();
		UserAccount userAccount = (UserAccount) userAccounts.toArray()[0];
		String username = userAccount.getUsername();
		
		Path checkUserDirTemplate = Paths.get(workspace,jobTemplates,checkUserDir);
		String checkUserDirTemplateDir = checkUserDirTemplate.toAbsolutePath().toString();
		
		p.setProperty("causalUser", username);
		p.setProperty("tmp", tempFolder);
		p.setProperty("results", resultFolder);
		p.setProperty("algorithm", algorithmResultFolder);
		
		List<String> cmdList = new LinkedList<>();
		cmdList.addAll(Arrays.asList(commands.split(";")));

		String datasets = null;
		for(int i=0;i<cmdList.size();i++){
			String cmd = cmdList.get(i);
			if(cmd.equalsIgnoreCase("--data")){
				datasets = cmdList.get(i+1);
				break;
			}
		}
		
		List<String> datasetList = new LinkedList<>();
		datasetList.addAll(Arrays.asList(datasets.split(",")));
		// The current dataset path is the one on the grid
		datasetList.forEach(dataset ->{
			// Extract fileName from the dataset
			Path dataPath = Paths.get(remotedataspace, username, dataFolder);
			String dataFile = dataset.replace(dataPath.toAbsolutePath().toString(), "");
			
			//The dataset's source path
			dataPath = Paths.get(workspace, username, dataFolder, dataFile);
			Path scriptPath = Paths.get(remoteworkspace, checkUserDirScript);
			String scriptDir = scriptPath.toAbsolutePath().toString() + username + ".sh";
			LOGGER.info("submitJobtoSlurm: checkUserDirScript: " + scriptDir);
			try {
				client.uploadDataset(
						checkUserDirTemplateDir, 
						p,
						scriptDir,
						dataPath.toAbsolutePath().toString(), 
						dataset);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		cmdList.add("--out");
		cmdList.add(tmpDirectory);

		String errorFileName = String.format("error_%s", fileName);
		Path error = Paths.get(tmpDirectory, errorFileName);
		
		// Redirect Error to File
		cmdList.add("2>");
		cmdList.add(error.toAbsolutePath().toString());
		
		StringBuilder sb = new StringBuilder();
		cmdList.forEach(cmd -> {
			sb.append(cmd);
			sb.append(" ");
		});
		LOGGER.info("Algorithm command: " + sb.toString());

		try {
			
			// Submit a job & Get remote job Id
			p.setProperty("email", userAccount.getPerson().getEmail());
			p.setProperty("cmd", sb.toString());
			Path causalJobTemplate = Paths.get(workspace,jobTemplates,causalJob);
			String causalJobTemplateDir = causalJobTemplate.toAbsolutePath().toString();
			Path scriptPath = Paths.get(remoteworkspace, username, runSlurmJobScript);
			String scriptDir = scriptPath.toAbsolutePath().toString() + queueId  + ".sh";
			LOGGER.info("submitJobtoSlurm: runSlurmJobScript: " + scriptDir);
			long pid = client.submitJob(
					causalJobTemplateDir, p, 
					scriptDir);
			
			JobQueueInfo queuedJobInfo = jobQueueInfoService.findOne(queueId);
			LOGGER.info("Set Job's pid to be: " + pid);
			queuedJobInfo.setPid(pid);
			jobQueueInfoService.saveJobIntoQueue(queuedJobInfo);

		} catch (Exception exception) {
			LOGGER.error("Algorithm did not run successfully.", exception);
		}

		return new AsyncResult<>(null);
	}

}
