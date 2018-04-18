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

import edu.pitt.dbmi.ccd.db.entity.JobQueue;
import java.util.regex.Pattern;

/**
 *
 * Apr 18, 2018 1:57:19 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface TaskService {

    public static final Pattern PIPE_PATTERN = Pattern.compile("\\|");
    public static final Pattern COLON_PATTERN = Pattern.compile(":");

    public void runTask(JobQueue jobQueue);

}