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
package edu.pitt.dbmi.ccd.job.queue.prop;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 *
 * Apr 17, 2018 10:56:27 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
@Component
@PropertySource("classpath:job-queue.properties")
public class JobQueueProperties {

    @Value("${ccd.server.workspace:}")
    private String workspaceDir;

    @Value("${ccd.executable.tetrad:}")
    private String tetradExecutable;

    @Value("#{'${ccd.java.option.jvm}'.split(',')}")
    private List<String> jvmOptions;

    public JobQueueProperties() {
    }

    public String getWorkspaceDir() {
        return workspaceDir;
    }

    public void setWorkspaceDir(String workspaceDir) {
        this.workspaceDir = workspaceDir;
    }

    public String getTetradExecutable() {
        return tetradExecutable;
    }

    public void setTetradExecutable(String tetradExecutable) {
        this.tetradExecutable = tetradExecutable;
    }

    public List<String> getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(List<String> jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

}
