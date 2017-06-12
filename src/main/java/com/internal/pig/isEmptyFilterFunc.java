package com.internal.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Simple Filter-Function to be used in Filter .. BY expression.
 * 
 * It's EvalFunc with return type Boolean data type.
 * 
 * @author rbordoloi
 *
 */
public class isEmptyFilterFunc extends FilterFunc {

  @Override public Boolean exec(Tuple input) throws IOException {
    try {
      Object values = input.get(0);
      if (values instanceof DataBag)
        return ((DataBag) values).size() == 0;
      else if (values instanceof Map)
        return ((Map<?, ?>) values).size() == 0;
      else {
        int errCode = 2102;
        String msg = "Cannot test a " + DataType.findTypeName(values) + " for emptiness.";
        throw new ExecException(msg, errCode, PigException.BUG);
      }
    } catch (ExecException ee) {
      throw ee;
    }
  }

}