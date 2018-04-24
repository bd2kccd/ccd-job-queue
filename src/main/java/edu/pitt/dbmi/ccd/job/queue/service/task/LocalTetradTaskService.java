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
import edu.pitt.dbmi.ccd.db.entity.JobInfo;
import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.service.JobQueueService;
import edu.pitt.dbmi.ccd.job.queue.service.CommandLineService;
import edu.pitt.dbmi.ccd.job.queue.service.FileSysService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

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

    @Autowired
    public LocalTetradTaskService(CommandLineService cmdLineService, FileSysService fileSysService, JobQueueService jobQueueService) {
        this.cmdLineService = cmdLineService;
        this.fileSysService = fileSysService;
        this.jobQueueService = jobQueueService;
    }

    @Async
    @Override
    public void runTask(JobQueue jobQueue) {
        jobQueueService.setStatusStarted(jobQueue);
        try {
            JobInfo jobInfo = jobQueue.getJobInfo();

            ProcessBuilder pb = new ProcessBuilder(cmdLineService.createCmdList(jobInfo, true));
            pb.redirectError(fileSysService.getErrorFile(jobInfo).toFile());

            Process process = pb.start();
            jobQueueService.setPID(Processes.processId(process), jobQueue);

            process.waitFor();

            jobQueueService.setStatusFinished(jobQueue);
        } catch (IOException | InterruptedException exception) {
            LOGGER.error("Job failed.", exception);

            jobQueueService.setStatusFailed(jobQueue);
        }
    }

    public void cleanUpFiles(JobQueue jobQueue, boolean success) throws IOException {
        JobInfo jobInfo = jobQueue.getJobInfo();

        Path outDir = fileSysService.getDirOut(jobInfo);
        String fileName = success
                ? jobInfo.getName() + "_graph.json"
                : jobInfo.getName() + ".error";
        Path source = fileSysService.getFileInDirOut(jobInfo, fileName);
        Path target = fileSysService.getFileInResultDir(jobInfo, fileName);

        if (Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        FileSystemUtils.deleteRecursively(outDir);
    }

}
