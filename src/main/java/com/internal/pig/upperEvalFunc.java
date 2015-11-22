package com.internal.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * EvalFunc class is the base class for all eval functions.
 * 
 * @author rbordoloi
 *
 */
public class upperEvalFunc extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
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

	/**
	 * Allow a UDF to specify type specific implementations of itself. For
	 * example, an implementation of arithmetic sum might have int and float
	 * implementations, since integer arithmetic performs much better than
	 * floating point arithmetic. Pig's typechecker will call this method and
	 * using the returned list plus the schema of the function's input data,
	 * decide which implementation of the UDF to use.
	 * 
	 * @return A List containing FuncSpec objects representing the EvalFunc
	 *         class which can handle the inputs corresponding to the schema in
	 *         the objects. Each FuncSpec should be constructed with a schema
	 *         that describes the input for that implementation. For example,
	 *         the sum function above would return two elements in its list:
	 *         <ol>
	 *         <li>FuncSpec(this.getClass().getName(), new Schema(new
	 *         Schema.FieldSchema(null, DataType.DOUBLE)))
	 *         <li>FuncSpec(IntSum.getClass().getName(), new Schema(new
	 *         Schema.FieldSchema(null, DataType.INTEGER)))
	 *         </ol>
	 *         This would indicate that the main implementation is used for
	 *         doubles, and the special implementation IntSum is used for ints.
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
				new Schema.FieldSchema(null, DataType.CHARARRAY))));
		return funcList;
	}
}