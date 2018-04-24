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
import edu.pitt.dbmi.ccd.job.queue.prop.JobQueueProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 20, 2018 4:11:27 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Service
public class FileSysService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSysService.class);

    private final String DATA_FOLDER = "data";
    private final String TMP_FOLDER = "tmp";
    private final String RESULT_FOLDER = "results";

    private final JobQueueProperties jobQueueProperties;

    @Autowired
    public FileSysService(JobQueueProperties jobQueueProperties) {
        this.jobQueueProperties = jobQueueProperties;
    }

    public Path getDirOut(JobInfo jobInfo) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = jobInfo.getUserAccount().getAccount();
        String jobName = jobInfo.getName();

        return Paths.get(rootDir, userFolder, TMP_FOLDER, jobName);
    }

    public Path getResultDir(JobInfo jobInfo) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = jobInfo.getUserAccount().getAccount();
        String jobName = jobInfo.getName();

        Path resultDir = Paths.get(rootDir, userFolder, RESULT_FOLDER, jobName);
        if (Files.notExists(resultDir, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Files.createDirectories(resultDir);
            } catch (IOException exception) {
                LOGGER.error("Unable create directory.", exception);
            }
        }

        return resultDir;
    }

    public Path getDataset(JobInfo jobInfo, File file) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = jobInfo.getUserAccount().getAccount();

        return Paths.get(rootDir, userFolder, DATA_FOLDER, file.getName());
    }

    public Path getErrorFile(JobInfo jobInfo) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = jobInfo.getUserAccount().getAccount();
        String jobName = jobInfo.getName();

        return Paths.get(rootDir, userFolder, TMP_FOLDER, jobName, jobName + ".error");
    }

    public Path getFileInDirOut(JobInfo jobInfo, String fileName) {
        return Paths.get(getDirOut(jobInfo).toString(), fileName);
    }

    public Path getFileInResultDir(JobInfo jobInfo, String fileName) {
        return Paths.get(getResultDir(jobInfo).toString(), fileName);
    }

}
