package com.internal.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * EvalFunc class is the base class for all eval functions.
 * 
 * @author rbordoloi
 *
 */
public class recordCountEvalFunc extends EvalFunc<String> {

  @Override public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      reporter.progress();
      String str = (String) input.get(0);
      return str.toUpperCase();
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}