package org.apache.s4.dispatcher.partitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

public class TestDefaultHashing {

	// a test for S4-30
	// from a list of inputs we compute hashes using the available algorithms and verify that none is negative (which would result in events not being dispatched)
	@Test
	public void test() throws IOException {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/hashingInput")));
		String line;
		while ((line = br.readLine()) !=null) {
			HashAlgorithm[] hashAlgorithms = HashAlgorithm.values();
			for (HashAlgorithm hashAlgorithm : hashAlgorithms) {
				Assert.assertTrue(((int)hashAlgorithm.hash(line)) >= 0);
				// even more inputs
				Assert.assertTrue(((int)hashAlgorithm.hash(new String(Base64.decodeBase64(line)))) >= 0);
			}
		}
		br.close();
	}
	
	

}
