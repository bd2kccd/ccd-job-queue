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
package edu.pitt.dbmi.ccd.queue.model;

/**
 * 
 * Aug 18, 2015 12:14:48 PM
 * 
 * @author Chirayu (Kong) Wongchokprasitti
 *
 */
public class JobQueueRequest {
	
	private String algorName;
	private String command;
	private String fileName;

	/**
	 * @param algorName
	 * @param command
	 * @param fileName
	 */
	public JobQueueRequest(String algorName, String command, String fileName) {
		this.algorName = algorName;
		this.command = command;
		this.fileName = fileName;
	}

	public String getAlgorName() {
		return algorName;
	}

	public void setAlgorName(String algorName) {
		this.algorName = algorName;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
