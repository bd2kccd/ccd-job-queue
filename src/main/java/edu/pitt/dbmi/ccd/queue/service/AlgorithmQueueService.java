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

import de.flapdoodle.embed.process.runtime.Processes;
import edu.pitt.dbmi.ccd.db.entity.JobQueueInfo;
import edu.pitt.dbmi.ccd.db.service.JobQueueInfoService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 16, 2015 11:41:02 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Profile("scheduler")
@Service
@EnableAsync
public class AlgorithmQueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlgorithmQueueService.class);

    private final JobQueueInfoService jobQueueInfoService;

    @Autowired(required = true)
    public AlgorithmQueueService(JobQueueInfoService queuedJobInfoService) {
        this.jobQueueInfoService = queuedJobInfoService;
    }

    @Async
    public Future<Void> runAlgorithmFromQueue(JobQueueInfo jobQueueInfo) {
        Long queueId = jobQueueInfo.getId();
        String fileName = jobQueueInfo.getFileName() + ".txt";
        String jsonFileName = jobQueueInfo.getFileName() + ".json";
        String commands = jobQueueInfo.getCommands();
        String tmpDirectory = jobQueueInfo.getTmpDirectory();
        String outputDirectory = jobQueueInfo.getOutputDirectory();

        List<String> cmdList = new LinkedList<>();
        cmdList.addAll(Arrays.asList(commands.split(";")));

        cmdList.add("--out");
        cmdList.add(tmpDirectory);

        StringBuilder sb = new StringBuilder();
        cmdList.forEach(cmd -> {
            sb.append(cmd);
            sb.append(" ");
        });
        LOGGER.info("Algorithm command: " + sb.toString());

        String errorFileName = String.format("error_%s", fileName);
        Path error = Paths.get(tmpDirectory, errorFileName);
        Path errorDest = Paths.get(outputDirectory, errorFileName);
        Path src = Paths.get(tmpDirectory, fileName);
        Path dest = Paths.get(outputDirectory, fileName);
        Path json = Paths.get(tmpDirectory, fileName);
        Path jsonDest = Paths.get(outputDirectory, fileName);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmdList);
            pb.redirectError(error.toFile());
            Process process = pb.start();

            //Get process Id
            Long pid = Processes.processId(process);
            JobQueueInfo queuedJobInfo = jobQueueInfoService.findOne(queueId);
            LOGGER.info("Set Job's pid to be: " + pid);
            queuedJobInfo.setPid(pid);
            jobQueueInfoService.saveJobIntoQueue(queuedJobInfo);

            process.waitFor();

            if (process.exitValue() == 0) {
                LOGGER.info(String.format("Moving txt file %s to %s", src, dest));
                Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info(String.format("Moving json file %s to %s", json, dest));
                Files.move(json, jsonDest, StandardCopyOption.REPLACE_EXISTING);
                Files.deleteIfExists(error);
            } else {
                LOGGER.info(String.format("Deleting tmp txt file %s", src));
                Files.deleteIfExists(src);
                LOGGER.info(String.format("Moving error file %s to %s", error, errorDest));
                Files.move(error, errorDest, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | InterruptedException exception) {
            LOGGER.error("Algorithm did not run successfully.", exception);
        }

        LOGGER.info("Delete Job ID from queue: " + queueId);
        jobQueueInfoService.deleteJobById(queueId);

        return new AsyncResult<>(null);
    }

}
