package com.internal.pig;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * RECORD-COUNT
 * 
 * An aggregate function is an eval function that takes a bag and returns a
 * scalar value.
 * 
 * One interesting and useful property of many aggregate functions is that they
 * can be computed incrementally in a distributed fashion. We call these
 * functions algebraic.
 * 
 * COUNT is an example of an algebraic function because we can count the number
 * of elements in a subset of the data and then sum the counts to produce a
 * final output. In the Hadoop world, this means that the partial computations
 * can be done by the map and combiner, and the final result can be computed by
 * the reducer.
 * 
 * It is very important for performance to make sure that aggregate functions
 * that are algebraic are implemented as such.
 * 
 * 
 * For a function to be algebraic, it needs to implement Algebraic interface
 * that consist of definition of three classes derived from EvalFunc.
 * 
 * The contract is that the exec function of the Initial class is called once
 * and is passed the original input tuple.
 * 
 * Its output is a tuple that contains partial results. The exec function of the
 * Intermed class can be called zero or more times and takes as its input a
 * tuple that contains partial results produced by the Initial class or by prior
 * invocations of the Intermed class and produces a tuple with another partial
 * result.
 * 
 * Finally, the exec function of the Final class is called and produces the
 * final result as a scalar type.
 * 
 * 
 * @author rbordoloi
 *
 */
public class countAlgebricEvalFunc extends EvalFunc<Long> implements Algebraic {
	@Override
	public Long exec(Tuple input) throws IOException {
		return count(input);
	}

	public String getFinal() {
		return Final.class.getName();
	}

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Intermed.class.getName();
	}

	static public class Initial extends EvalFunc<Tuple> {
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(count(input));
		}
	}

	static public class Intermed extends EvalFunc<Tuple> {
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(sum(input));
		}
	}

	static public class Final extends EvalFunc<Long> {
		public Long exec(Tuple input) throws IOException {
			return (Long) sum(input);
		}
	}

	static protected Long count(Tuple input) throws ExecException {
		Object values = input.get(0);
		if (values instanceof DataBag)
			return ((DataBag) values).size();
		else if (values instanceof Map)
			return new Long(((Map<?, ?>) values).size());
		return null;
	}

	static protected Long sum(Tuple input) throws ExecException,
			NumberFormatException {
		DataBag values = (DataBag) input.get(0);
		long sum = 0;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			sum += (Long) t.get(0);
		}
		return sum;
	}
}
