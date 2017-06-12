package com.internal.pig;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * TOKENIZE the records.
 * 
 * @author rbordoloi
 *
 */
public class tokenizeEvalFunc extends EvalFunc<DataBag> {
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory mBagFactory = BagFactory.getInstance();

  public DataBag exec(Tuple input) throws IOException {
    try {
      DataBag output = mBagFactory.newDefaultBag();
      Object o = input.get(0);
      if ((o instanceof String)) {
        throw new IOException("Expected input to be chararray, but  got " + o.getClass().getName());
      }
      StringTokenizer tok = new StringTokenizer((String) o, " \",()*", false);
      while (tok.hasMoreTokens())
        output.add(mTupleFactory.newTuple(tok.nextToken()));
      return output;
    } catch (ExecException ee) {
      // error handling goes here
    }
    return null;
  }
}