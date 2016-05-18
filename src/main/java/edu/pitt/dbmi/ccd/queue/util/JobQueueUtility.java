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
package edu.pitt.dbmi.ccd.queue.util;

import edu.pitt.dbmi.ccd.db.entity.JobQueueInfo;
import edu.pitt.dbmi.ccd.queue.model.AlgorithmJob;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * Aug 18, 2015 2:52:28 PM
 *
 * @author Chirayu (Kong) Wongchokprasitti
 *
 */
public class JobQueueUtility {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("MMM dd, yyyy HH:mm:ss");

    public static AlgorithmJob convertJobEntity2JobModel(JobQueueInfo job) {
        long addedJobTime = job.getAddedTime().getTime();
        String addedTime = TIME_FORMATTER.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(addedJobTime), ZoneId.systemDefault()));

        String status;
        switch (job.getStatus()) {
            case 0:
                status = "Queued";
                break;
            case 1:
                status = "Running";
                break;
            case 2:
                status = "Kill Request";
                break;
            default:
                status = "Unknown Status";
        }

        return new AlgorithmJob(job.getId(), job.getAlgorName(), job.getFileName(), status, addedTime);
    }

}
