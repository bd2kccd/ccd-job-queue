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
import edu.pitt.dbmi.ccd.db.service.AlgorithmTypeService;
import edu.pitt.dbmi.ccd.db.service.JobQueueService;
import edu.pitt.dbmi.ccd.db.service.JobStatusService;
import edu.pitt.dbmi.ccd.job.queue.service.task.TetradTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 *
 * Apr 18, 2018 1:55:13 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Component
public class ScheduledTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledTask.class);

    private final JobQueueService jobQueueService;
    private final JobStatusService jobStatusService;
    private final TetradTaskService tetradTaskService;

    @Autowired
    public ScheduledTask(JobQueueService jobQueueService, JobStatusService jobStatusService, TetradTaskService tetradTaskService) {
        this.jobQueueService = jobQueueService;
        this.jobStatusService = jobStatusService;
        this.tetradTaskService = tetradTaskService;
    }

    private void handleCanceledTasks() {
        jobQueueService.getRepository()
                .findByJobStatus(jobStatusService.findById(JobStatusService.CANCELED_ID))
                .forEach(jobQueue -> {
                });
    }

    private void handleFailedTasks() {
        jobQueueService.getRepository()
                .findByJobStatus(jobStatusService.findById(JobStatusService.FAILED_ID))
                .forEach(jobQueue -> {
                    LOGGER.info("Dequeue job: " + jobQueue.getId());
                    jobQueueService.getRepository().delete(jobQueue);
                });
    }

    private void handleFinishedTasks() {
        jobQueueService.getRepository()
                .findByJobStatus(jobStatusService.findById(JobStatusService.FINISHED_ID))
                .forEach(jobQueue -> {
                    LOGGER.info("Dequeue job: " + jobQueue.getId());
                    jobQueueService.getRepository().delete(jobQueue);
                });
    }

    private void handleQueuedTasks() {
        jobQueueService.getRepository()
                .findByJobStatus(jobStatusService.findById(JobStatusService.QUEUED_ID))
                .forEach(jobQueue -> {
                    LOGGER.info("Running job: " + jobQueue.getId());
                    if (isTetradJob(jobQueue)) {
                        tetradTaskService.runTask(jobQueue);
                    }
                });
    }

    private void handleStartedTasks() {
    }

    private boolean isTetradJob(JobQueue jobQueue) {
        long id = jobQueue.getJobInfo().getAlgorithmType().getId();

        return id == AlgorithmTypeService.TETRAD_ID;
    }

    @Scheduled(fixedRateString = "${ccd.schedule.rate.fixed:5000}")
    public void runQueue() {
        handleCanceledTasks();
        handleFailedTasks();
        handleFinishedTasks();
        handleQueuedTasks();
        handleStartedTasks();
    }

}
