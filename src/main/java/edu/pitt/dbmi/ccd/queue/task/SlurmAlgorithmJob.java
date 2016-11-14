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
package edu.pitt.dbmi.ccd.queue.task;

import edu.pitt.dbmi.ccd.connection.slurm.JobStat;
import edu.pitt.dbmi.ccd.connection.slurm.JobStatus;
import edu.pitt.dbmi.ccd.db.entity.JobQueueInfo;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.JobQueueInfoService;
import edu.pitt.dbmi.ccd.queue.service.AlgorithmSlurmService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 *
 * May 24, 2016 6:20:37 PM
 *
 * @author Chirayu (Kong) Wongchokprasitti
 *
 */
@Profile("slurm")
@Component
@EnableScheduling
public class SlurmAlgorithmJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlurmAlgorithmJob.class);

    private final int queueSize;

    private final JobQueueInfoService jobQueueInfoService;

    private final AlgorithmSlurmService algorithmSlurmService;

    @Autowired(required = true)
    public SlurmAlgorithmJob(@Value("${ccd.queue.size:5}") int queueSize,
            JobQueueInfoService jobQueueInfoService,
            AlgorithmSlurmService algorithmQueueService) {
        this.queueSize = queueSize;
        this.jobQueueInfoService = jobQueueInfoService;
        this.algorithmSlurmService = algorithmQueueService;
    }

    @Scheduled(fixedRate = 5000)
    public void manageJobsInQueue() {
        List<JobStatus> finishedJobList = algorithmSlurmService.getFinishedJobs();
        Map<Long, JobStatus> finishedJobMap = new HashMap<>();
        if (finishedJobList != null && !finishedJobList.isEmpty()) {
            finishedJobList.forEach(jobStatus -> {
                finishedJobMap.put(new Long(jobStatus.getJobId()), jobStatus);
            });
        }

        int numRunningJobs = 0;

        List<JobQueueInfo> runningJobList = jobQueueInfoService.findByStatus(1);
        for (JobQueueInfo job : runningJobList) {
            Long pid = job.getPid();
            if (pid != null && finishedJobMap.containsKey(pid)) {
                if (finishedJobMap.containsKey(pid)) {
                    Long queueId = job.getId();
                    jobQueueInfoService.deleteJobById(queueId);

                    JobStatus status = finishedJobMap.get(pid);
                    JobStat stat = algorithmSlurmService.getJobStat(pid);
                    Set<UserAccount> userAccounts = job.getUserAccounts();
                    UserAccount userAccount
                            = (UserAccount) userAccounts.toArray()[0];
                    String username = userAccount.getUsername();
                    LOGGER.info("JobUsage{"
                            + "JobId=" + queueId + ", "
                            + "HPCjobId=" + pid + ", "
                            + "User='" + username + "', "
                            + "State='" + status.getState() + "', "
                            + "Elapsed='" + status.getTime() + "', "
                            + "Start='" + stat.getStart() + "', "
                            + "End='" + stat.getEnd() + "', "
                            + "Partition='" + status.getPartition() + "', "
                            + "AllocCPUS=" + stat.getAllocCPUS() + ", "
                            + "AllocNodes=" + stat.getAllocNodes() + ", "
                            + "NodeList='" + status.getNodelist() + "'}");

                    algorithmSlurmService.downloadJobResult(job);
                } else {
                    numRunningJobs++;
                }
            }
        }

        if (numRunningJobs < queueSize) {
            // Waiting list to execute
            List<JobQueueInfo> jobList = jobQueueInfoService.findByStatus(0);
            if (!jobList.isEmpty()) {
                // Execute one at a time
                JobQueueInfo jobQueueInfo = jobList.get(0);
                LOGGER.info("Run Job ID: " + jobQueueInfo.getId());

                try {
                    LOGGER.info("Set Job's status to be 1 (running): " + jobQueueInfo.getId());
                    jobQueueInfo.setStatus(1);
                    jobQueueInfoService.saveJobIntoQueue(jobQueueInfo);

                    algorithmSlurmService.submitJobtoSlurm(jobQueueInfo);
                } catch (Exception exception) {
                    LOGGER.error("Unable to run " + jobQueueInfo.getAlgorName(), exception);
                }
            }
        }

        // Waiting list to terminate
        List<JobQueueInfo> jobList = jobQueueInfoService.findByStatus(2);
        jobList.forEach(job -> {
            killJob(job.getId());
        });

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
                LOGGER.info("Delete Job ID by user from queue: " + queueId);
                jobQueueInfoService.deleteJobById(queueId);

                algorithmSlurmService.cancelSlurmJob(jobQueueInfo);

            }
        }
    }
}
