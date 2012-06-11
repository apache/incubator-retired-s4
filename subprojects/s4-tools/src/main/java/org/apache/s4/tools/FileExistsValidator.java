package org.apache.s4.tools;

import java.io.File;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class FileExistsValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (!new File(value).exists()) {
            throw new ParameterException("File with path [" + value + "] specified in [" + name + "] does not exist");
        }

    }

}
