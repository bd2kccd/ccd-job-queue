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
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.db.service.AlgorithmTypeService;
import edu.pitt.dbmi.ccd.job.queue.prop.JobQueueProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
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
    private final String RESULT_FOLDER = "results";

    private final JobQueueProperties jobQueueProperties;

    @Autowired
    public FileSysService(JobQueueProperties jobQueueProperties) {
        this.jobQueueProperties = jobQueueProperties;
    }

    public Path getUserHomeDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        Path dir = Paths.get(rootDir, userFolder);
        if (Files.notExists(dir)) {
            try {
                Files.createDirectory(dir);
            } catch (IOException exception) {
                LOGGER.error("Unable to create user home directory.", exception);
            }
        }

        return dir;
    }

    public Path getUserDataDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        Path dir = Paths.get(rootDir, userFolder, DATA_FOLDER);
        if (Files.notExists(dir)) {
            try {
                Files.createDirectory(dir);
            } catch (IOException exception) {
                LOGGER.error("Unable to create directory for data.", exception);
            }
        }

        return dir;
    }

    public Path getUserResultDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        Path dir = Paths.get(rootDir, userFolder, RESULT_FOLDER);
        if (Files.notExists(dir)) {
            try {
                Files.createDirectory(dir);
            } catch (IOException exception) {
                LOGGER.error("Unable to create directory for results.", exception);
            }
        }

        return dir;
    }

    public Path getOutputDirectory(JobInfo jobInfo) {
        String resultDir = getUserResultDirectory(jobInfo.getUserAccount()).toString();
        String subDir = jobInfo.getName();

        return Paths.get(resultDir, subDir);
    }

    public Path getErrorFile(JobInfo jobInfo) {
        long id = jobInfo.getAlgorithmType().getId();
        String outDir = getOutputDirectory(jobInfo).toString();
        String errFile = (id == AlgorithmTypeService.TETRAD_ID)
                ? "tetrad.error"
                : "tdi.error";

        return Paths.get(outDir, errFile);
    }

    public Path getDataset(UserAccount userAccount, File file) {
        String dataDir = getUserDataDirectory(userAccount).toString();
        String fileName = file.getName();

        return Paths.get(dataDir, fileName);
    }

    public List<Path> getFilesInResultDirectory(String subFolder, UserAccount userAccount) throws IOException {
        String resultDir = getUserResultDirectory(userAccount).toString();

        return Files.walk(Paths.get(resultDir, subFolder))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
    }

}
