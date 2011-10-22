package org.apache.s4.comm.topology;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 
 * Utility to serialize/deserialize data in ZK. <br/>
 * Using Json format and Gson library. 
 * TODO: Explore other libraries like jackson much richer features.
 * Gson needs no-arg constructor to work with without additional work
 */
public class ZNRecordSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data != null) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String json = gson.toJson(data);
            if (json != null) {
                return json.getBytes();
            }
        }
        return new byte[0];
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        ZNRecord record = null;
        if (bytes != null) {
            Gson gson = new Gson();
            record = gson.fromJson(new String(bytes), ZNRecord.class);
        }
        return record;
    }

}
