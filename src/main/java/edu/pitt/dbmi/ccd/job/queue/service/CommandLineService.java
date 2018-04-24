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
import edu.pitt.dbmi.ccd.db.entity.TetradDataFile;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.FileGroupService;
import edu.pitt.dbmi.ccd.db.service.TetradDataFileService;
import edu.pitt.dbmi.ccd.job.queue.prop.JobQueueProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 20, 2018 10:31:29 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Service
public class CommandLineService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineService.class);

    public static final Pattern PIPE_PATTERN = Pattern.compile("\\|");
    public static final Pattern COLON_PATTERN = Pattern.compile(":");

    private final JobQueueProperties jobQueueProperties;
    private final FileGroupService fileGroupService;
    private final TetradDataFileService tetradDataFileService;
    private final FileSysService fileSysService;

    @Autowired
    public CommandLineService(JobQueueProperties jobQueueProperties, FileGroupService fileGroupService, TetradDataFileService tetradDataFileService, FileSysService fileSysService) {
        this.jobQueueProperties = jobQueueProperties;
        this.fileGroupService = fileGroupService;
        this.tetradDataFileService = tetradDataFileService;
        this.fileSysService = fileSysService;
    }

    public List<String> createCmdList(JobInfo jobInfo, boolean local) {
        List<String> cmdList = new LinkedList<>();
        addCommon(jobInfo, cmdList);
        if (local) {
            addLocalData(jobInfo, cmdList);
            addLocalDirOut(jobInfo, cmdList);
        }

        return cmdList;
    }

    private void addDelimiterAndVariableType(TetradDataFile dataFile, List<String> cmdList) {
        cmdList.add("--delimiter");
        cmdList.add(dataFile.getDataDelimiter().getShortName());

        cmdList.add("--data-type");
        cmdList.add(dataFile.getVariableType().getShortName());
    }

    private void addLocalDirOut(JobInfo jobInfo, List<String> cmdList) {
        Path dirOut = fileSysService.getDirOut(jobInfo);
        if (Files.notExists(dirOut, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Files.createDirectory(dirOut);
            } catch (IOException exception) {
                LOGGER.error("Unable to create temp directory.", exception);
            }
        }

        cmdList.add("--out");
        cmdList.add(dirOut.toString());
    }

    private void addLocalData(JobInfo jobInfo, List<String> cmdList) {
        UserAccount userAccount = jobInfo.getUserAccount();

        if (jobInfo.isSingleDataset()) {
            TetradDataFile dataFile = tetradDataFileService.getRepository()
                    .findByIdAndUserAccount(jobInfo.getDatasetId(), userAccount);
            if (dataFile != null) {
                cmdList.add("--dataset");
                cmdList.add(fileSysService.getDataset(jobInfo, dataFile.getFile()).toString());

                addDelimiterAndVariableType(dataFile, cmdList);
            }
        } else {
            List<File> files = fileGroupService.getRepository()
                    .getFiles(jobInfo.getDatasetId(), userAccount);
            List<TetradDataFile> dataFiles = tetradDataFileService.getRepository()
                    .findByFileIn(files);
            if (!dataFiles.isEmpty()) {
                String dataStr = dataFiles.stream()
                        .map(e -> fileSysService.getDataset(jobInfo, e.getFile()).toString())
                        .collect(Collectors.joining(","));

                cmdList.add("--dataset");
                cmdList.add(dataStr);

                Optional<TetradDataFile> opt = dataFiles.stream().findFirst();
                if (opt.isPresent()) {
                    addDelimiterAndVariableType(opt.get(), cmdList);
                }
            }
        }
    }

    private void addParameter(JobInfo jobInfo, List<String> cmdList) {
        Arrays.stream(PIPE_PATTERN.split(jobInfo.getAlgoParam()))
                .forEach(e -> {
                    String[] keyVal = COLON_PATTERN.split(e);
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

    private void addCommon(JobInfo jobInfo, List<String> cmdList) {
        cmdList.add("java");

        // jvm options
        cmdList.addAll(jobQueueProperties.getJvmOptions());

        // jar file
        cmdList.add("-jar");
        cmdList.add(jobQueueProperties.getTetradExecutable());

        // algorithm parameters
        addParameter(jobInfo, cmdList);

        // output options
        cmdList.add("--prefix");
        cmdList.add(jobInfo.getName());

        cmdList.add("--json-graph");
        cmdList.add("--skip-latest");
    }

}
