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

import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.runtime.Processes;
import edu.pitt.dbmi.ccd.db.entity.JobQueueInfo;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.JobQueueInfoService;
import edu.pitt.dbmi.ccd.db.service.UserAccountService;
import edu.pitt.dbmi.ccd.queue.model.AlgorithmJob;
import edu.pitt.dbmi.ccd.queue.util.JobQueueUtility;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 *
 * Jul 31, 2015 11:19:59 AM
 *
 * @author Chirayu (Kong) Wongchokprasitti
 *
 */
@Service
public class JobQueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobQueueService.class);

    private final JobQueueInfoService jobQueueInfoService;

    private final UserAccountService userAccountService;

    private final AlgorithmService algorithmService;

    private final int queueSize;

    private final boolean disableJobScheduler;

    @Autowired(required = true)
    public JobQueueService(
            JobQueueInfoService jobQueueInfoService,
            UserAccountService userAccountService,
            AlgorithmService algorithmService,
            @Value("${ccd.queue.size:1}") int queueSize,
            @Value("${ccd.disable.scheduler:true}") boolean disableJobScheduler) {
        this.jobQueueInfoService = jobQueueInfoService;
        this.userAccountService = userAccountService;
        this.algorithmService = algorithmService;
        this.queueSize = queueSize;
        this.disableJobScheduler = disableJobScheduler;
    }

    @Scheduled()
    public void executeJobInQueue() {
        System.out.println(new Date(System.currentTimeMillis()));
        if (disableJobScheduler) {
            return;
        }

        Platform platform = Platform.detect();
        List<JobQueueInfo> jobsToSave = new LinkedList<>();
        List<JobQueueInfo> runningJobList = jobQueueInfoService.findByStatus(1);
        runningJobList.forEach(job -> {
            Long pid = job.getPid();
            if (pid == null || !Processes.isProcessRunning(platform, pid)) {
                job.setStatus(2);  // request to kill
                jobsToSave.add(job);
            }
        });

        jobQueueInfoService.saveAll(jobsToSave);

        runningJobList = jobQueueInfoService.findByStatus(1);
        int numRunningJobs = runningJobList.size();
        if (numRunningJobs > 0 && numRunningJobs < queueSize) {
            // Waiting list to execute
            List<JobQueueInfo> jobList = jobQueueInfoService.findByStatus(0);

            // Execute one at a time
            JobQueueInfo jobQueueInfo = jobList.get(0);
            LOGGER.info("Run Job ID: " + jobQueueInfo.getId());
            try {
                LOGGER.info("Set Job's status to be 1 (running): " + jobQueueInfo.getId());
                jobQueueInfo.setStatus(1);
                jobQueueInfoService.saveJobIntoQueue(jobQueueInfo);

                algorithmService.runAlgorithmFromQueue(jobQueueInfo);
            } catch (Exception exception) {
                LOGGER.error("Unable to run " + jobQueueInfo.getAlgorName(), exception);
            }
        }

        // Waiting list to terminate
        List<JobQueueInfo> jobList = jobQueueInfoService.findByStatus(2);
        jobList.forEach(job -> {
            killJob(job.getId());
        });
    }

    public List<AlgorithmJob> createJobQueueList(String username) {
        List<AlgorithmJob> listItems = new ArrayList<AlgorithmJob>();

        UserAccount userAccount = userAccountService.findByUsername(username);
        List<JobQueueInfo> listJobs = jobQueueInfoService.findByUserAccounts(Collections.singleton(userAccount));
        listJobs.forEach(job -> {
            AlgorithmJob algorithmJob = JobQueueUtility.convertJobEntity2JobModel(job);
            listItems.add(algorithmJob);
        });
        return listItems;
    }

    public AlgorithmJob removeJobQueue(Long queueId) {
        JobQueueInfo job = jobQueueInfoService.findOne(queueId);
        if (job == null) {
            return null;
        }
        job.setStatus(2);
        jobQueueInfoService.saveJobIntoQueue(job);
        return JobQueueUtility.convertJobEntity2JobModel(job);
    }

    private void killJob(Long queueId) {

        JobQueueInfo jobQueueInfo = jobQueueInfoService.findOne(queueId);
        if (jobQueueInfo.getStatus() == 0) {
            LOGGER.info("Delete Job ID by user from queue: " + queueId);
            jobQueueInfoService.deleteJobById(queueId);
        } else {
            Long pid = jobQueueInfo.getPid();
            if (pid == null) {
                LOGGER.info("Delete Job ID by user from queue: " + queueId);
                jobQueueInfoService.deleteJobById(queueId);
            } else {
                Platform platform = Platform.detect();
                System.out.println(
                        "Processes.isProcessRunning(platform, pid):" + Processes.isProcessRunning(platform, pid));
                if (Processes.isProcessRunning(platform, pid)) {
                    /*
                     * ISupportConfig support = null; IStreamProcessor output =
                     * null;
                     */
                    List<String> commands = new LinkedList<>();
                    if (platform == Platform.Windows) {
                        // return Processes.tryKillProcess(support, platform,
                        // output, pid.intValue());
                        commands.add("taskkill");
                        commands.add("/pid");
                        commands.add(String.valueOf(pid));
                        commands.add("/f");
                        commands.add("/t");
                    } else {
                        // return Processes.killProcess(support, platform,
                        // output, pid.intValue());
                        commands.add("kill");
                        commands.add("-9");
                        commands.add(String.valueOf(pid));
                    }
                    LOGGER.info("Kill Job Queue Id: " + jobQueueInfo.getId());
                    jobQueueInfo.setStatus(2);
                    jobQueueInfoService.saveJobIntoQueue(jobQueueInfo);
                    ProcessBuilder pb = new ProcessBuilder(commands);
                    try {
                        pb.start();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        LOGGER.error("Request to kill an algorithm job did not run successfully.", e);
                    }
                } else {
                    LOGGER.info("Job does not exist, delete Job ID from queue: " + queueId);
                    jobQueueInfoService.deleteJobById(queueId);
                }
            }
        }
    }
}
