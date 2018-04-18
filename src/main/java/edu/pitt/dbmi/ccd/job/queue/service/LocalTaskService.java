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

import edu.pitt.dbmi.ccd.db.entity.JobInfo;
import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import edu.pitt.dbmi.ccd.db.service.AlgorithmTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 *
 * Apr 18, 2018 2:08:41 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Profile("local")
@Service
public class LocalTaskService implements TaskService {

    private final TetradTaskService tetradTaskService;

    @Autowired
    public LocalTaskService(TetradTaskService tetradTaskService) {
        this.tetradTaskService = tetradTaskService;
    }

    @Override
    public void runTask(JobQueue jobQueue) {
        JobInfo jobInfo = jobQueue.getJobInfo();
        switch (jobInfo.getAlgorithmType().getShortName()) {
            case AlgorithmTypeService.TETRAD_SHORT_NAME:
                tetradTaskService.runTaskLocal(jobQueue);
                break;
        }
    }

}
