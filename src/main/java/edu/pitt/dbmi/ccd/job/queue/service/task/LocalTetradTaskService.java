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
package edu.pitt.dbmi.ccd.job.queue.service.task;

import de.flapdoodle.embed.process.runtime.Processes;
import edu.pitt.dbmi.ccd.db.entity.FileFormat;
import edu.pitt.dbmi.ccd.db.entity.JobInfo;
import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.FileFormatService;
import edu.pitt.dbmi.ccd.db.service.JobLocationService;
import edu.pitt.dbmi.ccd.db.service.JobQueueService;
import edu.pitt.dbmi.ccd.db.service.JobResultService;
import edu.pitt.dbmi.ccd.job.queue.service.CommandLineService;
import edu.pitt.dbmi.ccd.job.queue.service.FileSysService;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 24, 2018 3:40:18 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Profile("local")
@Service
public class LocalTetradTaskService extends AbstractTetradTask implements TetradTaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTetradTaskService.class);

    private final CommandLineService cmdLineService;
    private final FileSysService fileSysService;
    private final JobQueueService jobQueueService;
    private final JobResultService jobResultService;
    private final JobLocationService jobLocationService;
    private final FileFormatService fileFormatService;

    @Autowired
    public LocalTetradTaskService(CommandLineService cmdLineService, FileSysService fileSysService, JobQueueService jobQueueService, JobResultService jobResultService, JobLocationService jobLocationService, FileFormatService fileFormatService) {
        this.cmdLineService = cmdLineService;
        this.fileSysService = fileSysService;
        this.jobQueueService = jobQueueService;
        this.jobResultService = jobResultService;
        this.jobLocationService = jobLocationService;
        this.fileFormatService = fileFormatService;
    }

    @Async
    @Override
    public void runTask(JobQueue jobQueue) {
        jobQueueService.setStatusStarted(jobQueue);
        try {
            JobInfo jobInfo = jobQueue.getJobInfo();

            // set location where the job is running
            jobInfo.setJobLocation(jobLocationService.findByShortName(JobLocationService.LOCAL_SHORT_NAME));

            ProcessBuilder pb = new ProcessBuilder(cmdLineService.createCmdList(jobInfo, true));
            pb.redirectError(fileSysService.getErrorFile(jobInfo).toFile());

            Process process = pb.start();
            Long pid = Processes.processId(process);
            LOGGER.info("Running task PID: " + pid);
            jobQueueService.setPID(pid, jobQueue);

            process.waitFor();

            jobQueueService.setStatusFinished(jobQueue);
        } catch (IOException | InterruptedException exception) {
            LOGGER.error("Job failed.", exception);

            jobQueueService.setStatusFailed(jobQueue);
        }
    }

    @Override
    public void collectResultFiles(JobInfo jobInfo) {
        UserAccount userAccount = jobInfo.getUserAccount();
        String subFolder = jobInfo.getName();

        List<Path> files = new LinkedList<>();
        try {
            files.addAll(fileSysService.getFilesInResultDirectory(subFolder, userAccount));
        } catch (IOException exception) {
            LOGGER.error("", exception);
        }

        try {
            FileFormat fileFormat = fileFormatService.findByShortName(FileFormatService.TETRAD_RESULT_SHORT_NAME);
            jobResultService.addResultFiles(jobInfo, files, fileFormat, userAccount);
        } catch (Exception exception) {
            LOGGER.error("", exception);
        }

    }

}
