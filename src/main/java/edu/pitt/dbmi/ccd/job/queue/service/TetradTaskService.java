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

import edu.pitt.dbmi.ccd.db.entity.File;
import edu.pitt.dbmi.ccd.db.entity.JobInfo;
import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.FileGroupService;
import edu.pitt.dbmi.ccd.db.service.TetradDataFileService;
import edu.pitt.dbmi.ccd.job.queue.prop.JobQueueProperties;
import edu.pitt.dbmi.ccd.job.queue.service.filesys.FileManagementService;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
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

    private final String DATA_FOLDER = "data";

    private final JobQueueProperties jobQueueProperties;
    private final FileGroupService fileGroupService;
    private final TetradDataFileService tetradDataFileService;
    private final FileManagementService fileManagementService;

    @Autowired
    public TetradTaskService(JobQueueProperties jobQueueProperties, FileGroupService fileGroupService, TetradDataFileService tetradDataFileService, FileManagementService fileManagementService) {
        this.jobQueueProperties = jobQueueProperties;
        this.fileGroupService = fileGroupService;
        this.tetradDataFileService = tetradDataFileService;
        this.fileManagementService = fileManagementService;
    }

    @Async
    public void runTaskLocal(JobQueue jobQueue) {
        JobInfo jobInfo = jobQueue.getJobInfo();
    }

    public String createLocalCmd(JobInfo jobInfo) {
        List<String> cmdList = new LinkedList<>();
        addCommonCmd(jobInfo, cmdList);
        addLocalDataCmd(jobInfo, cmdList);

        return cmdList.stream().collect(Collectors.joining(" "));
    }

    private void addLocalDataCmd(JobInfo jobInfo, List<String> cmdList) {
        UserAccount userAccount = jobInfo.getUserAccount();

        cmdList.add("--dataset");

        if (jobInfo.isSingleDataset()) {
            File file = tetradDataFileService.getRepository()
                    .getFile(jobInfo.getDatasetId(), userAccount);
            cmdList.add(fileManagementService.getLocalFile(file, userAccount).toString());
        } else {
            List<File> files = fileGroupService.getRepository()
                    .getFiles(jobInfo.getDatasetId(), userAccount);
            String dataFile = files.stream()
                    .map(e -> fileManagementService.getLocalFile(e, userAccount).toString())
                    .collect(Collectors.joining(","));
            cmdList.add(dataFile);
        }
    }

    private void addParameterCmd(JobInfo jobInfo, List<String> cmdList) {
        Arrays.stream(TaskService.PIPE_PATTERN.split(jobInfo.getAlgoParam()))
                .forEach(e -> {
                    String[] keyVal = TaskService.COLON_PATTERN.split(e);
                    if (keyVal.length == 2) {
                        String key = String.format("--%s", keyVal[0]);
                        String val = keyVal[1];
                        switch (val) {
                            case "true":
                                cmdList.add(key);
                                break;
                            case "false":
                                // do nothing
                                break;
                            default:
                                cmdList.add(key);
                                cmdList.add(val);
                        }
                    }
                });
    }

    private void addCommonCmd(JobInfo jobInfo, List<String> cmdList) {
        cmdList.add("java");

        // jvm options
        cmdList.addAll(jobQueueProperties.getJvmOptions());

        // jar file
        cmdList.add("-jar");
        cmdList.add(jobQueueProperties.getTetradExecutable());

        // algorithm parameters
        addParameterCmd(jobInfo, cmdList);
    }

}
