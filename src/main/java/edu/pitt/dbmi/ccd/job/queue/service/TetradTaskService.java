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
package edu.pitt.dbmi.ccd.job.queue.service;

import de.flapdoodle.embed.process.runtime.Processes;
import edu.pitt.dbmi.ccd.db.entity.JobInfo;
import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.service.JobInfoService;
import edu.pitt.dbmi.ccd.db.service.JobQueueService;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 18, 2018 1:47:00 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Service
public class TetradTaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TetradTaskService.class);

    private final CommandLineService cmdLineService;
    private final FileSysService fileSysService;
    private final JobInfoService jobInfoService;
    private final JobQueueService jobQueueService;

    @Autowired
    public TetradTaskService(CommandLineService cmdLineService, FileSysService fileSysService, JobInfoService jobInfoService, JobQueueService jobQueueService) {
        this.cmdLineService = cmdLineService;
        this.fileSysService = fileSysService;
        this.jobInfoService = jobInfoService;
        this.jobQueueService = jobQueueService;
    }

    @Async
    public void runTaskLocal(JobQueue jobQueue) {
        LOGGER.info("Run Job ID: " + jobQueue.getId());

        JobInfo jobInfo = jobQueue.getJobInfo();
//        jobInfo = jobInfoService.setStartJob(jobInfo);
        try {
            ProcessBuilder pb = new ProcessBuilder(cmdLineService.createCmdList(jobInfo, true));
            pb.redirectError(fileSysService.getErrorFile(jobInfo).toFile());
            Process process = pb.start();

            Long pid = Processes.processId(process);
            jobQueueService.setPID(pid, jobQueue);
            LOGGER.info("Set Job's pid to be: " + pid);

            process.waitFor();

            if (process.exitValue() == 0) {

            }
        } catch (IOException | InterruptedException exception) {
            LOGGER.error("Job failed.", exception);

            jobInfoService.setTerminateJob(jobInfo);
        }
    }
}
