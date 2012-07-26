/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.comm.topology;

import java.util.concurrent.Callable;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;
/**
 * 
 * Overwriting the ZKclient since the org.I0Itec.zkclient.ZkClient does not expose some important methods
 */
public class ZkClient extends org.I0Itec.zkclient.ZkClient {

	public ZkClient(IZkConnection connection, int connectionTimeout,
			ZkSerializer zkSerializer) {
		super(connection, connectionTimeout, zkSerializer);
	}

	public ZkClient(IZkConnection connection, int connectionTimeout) {
		super(connection, connectionTimeout);
	}

	public ZkClient(IZkConnection connection) {
		super(connection);
	}

	public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
		super(zkServers, sessionTimeout, connectionTimeout);
	}

	public ZkClient(String zkServers, int connectionTimeout) {
		super(zkServers, connectionTimeout);
	}

	public ZkClient(String serverstring) {
		super(serverstring);
	}

	public IZkConnection getConnection() {
		return _connection;
	}
	
	public long getSessionId(){
		return ((ZkConnection)_connection).getZookeeper().getSessionId();
	}

	public Stat getStat(final String path) {
		Stat stat = retryUntilConnected(new Callable<Stat>() {

			@Override
			public Stat call() throws Exception {
				Stat stat = ((ZkConnection) _connection).getZookeeper().exists(
						path, false);
				return stat;
			}
		});

		return stat;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Object> T readData(String path,
			boolean returnNullIfPathNotExists) {
		T data = null;
		try {
			data = (T) readData(path, null);
		} catch (ZkNoNodeException e) {
			if (!returnNullIfPathNotExists) {
				throw e;
			}
		}
		return data;
	}

}
