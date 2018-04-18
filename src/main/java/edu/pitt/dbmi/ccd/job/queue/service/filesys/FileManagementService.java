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
package edu.pitt.dbmi.ccd.job.queue.service.filesys;

import edu.pitt.dbmi.ccd.db.entity.File;
import edu.pitt.dbmi.ccd.db.entity.UserAccount;
import edu.pitt.dbmi.ccd.job.queue.prop.JobQueueProperties;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 18, 2018 6:18:20 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Service
public class FileManagementService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileManagementService.class);

    private final String DATA_FOLDER = "data";
    private final String RESULT_FOLDER = "results";

    private final JobQueueProperties jobQueueProperties;

    @Autowired
    public FileManagementService(JobQueueProperties jobQueueProperties) {
        this.jobQueueProperties = jobQueueProperties;
    }

    public Path getLocalFile(File file, UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();
        String fileName = file.getName();

        return Paths.get(rootDir, userFolder, DATA_FOLDER, fileName);
    }

    public Path getUserHomeDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        return Paths.get(rootDir, userFolder);
    }

    public Path getUserDataDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        return Paths.get(rootDir, userFolder, DATA_FOLDER);
    }

    public Path getUserResultDirectory(UserAccount userAccount) {
        String rootDir = jobQueueProperties.getWorkspaceDir();
        String userFolder = userAccount.getAccount();

        return Paths.get(rootDir, userFolder, RESULT_FOLDER);
    }

}
