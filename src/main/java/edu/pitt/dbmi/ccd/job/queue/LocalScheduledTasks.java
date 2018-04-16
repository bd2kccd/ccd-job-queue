/*
 * Copyright (C) 2018 University of Pittsburgh.
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
package edu.pitt.dbmi.ccd.job.queue;

import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.entity.JobStatus;
import edu.pitt.dbmi.ccd.db.service.JobQueueService;
import edu.pitt.dbmi.ccd.db.service.JobStatusService;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 *
 * Apr 16, 2018 2:58:27 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Profile("local")
@Component
public class LocalScheduledTasks {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalScheduledTasks.class);

    private final LocalRunTaskService localRunTaskService;
    private final JobQueueService jobQueueService;
    private final JobStatusService jobStatusService;

    @Autowired
    public LocalScheduledTasks(LocalRunTaskService localRunTaskService, JobQueueService jobQueueService, JobStatusService jobStatusService) {
        this.localRunTaskService = localRunTaskService;
        this.jobQueueService = jobQueueService;
        this.jobStatusService = jobStatusService;
    }

    @Scheduled(fixedRateString = "${ccd.schedule.rate.fixed:5000}")
    public void runQueue() {
        executeTasks();
    }

    private void executeTasks() {
        JobStatus jobStatus = jobStatusService
                .findByShortName(JobStatusService.QUEUE_SHORT_NAME);

        List<JobQueue> jobs = jobQueueService.getRepository()
                .findByJobStatus(jobStatus);
        jobs.forEach(job -> {
        });
    }

}
