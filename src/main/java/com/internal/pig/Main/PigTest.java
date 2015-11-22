package com.internal.pig.Main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class PigTest {

	public static void main(String[] args) throws IOException {
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);

		// pigServer.registerJar("/mylocation/tokenize.jar");
		runMyQuery(pigServer, "/rpal/Movies.txt");
	}

	private static void runMyQuery(PigServer pigServer, String inputFile)
			throws IOException {
		pigServer.registerQuery("A = load '" + inputFile + "' using PigStorage(',');");
		pigServer
				.registerQuery("B = group A all;");
		pigServer
				.registerQuery("C = FOREACH B GENERATE 'K1' AS key, (DOUBLE)COUNT(A) AS ct_MOVIES;");

//		pigServer.store("C", "/rpal/Movies_out.txt");
		Iterator<Tuple> it = pigServer.openIterator("C");
		
		double i = 0;
		int colCount = 0;
		if (it.hasNext()) {
			Tuple xx = it.next();
			i = (Double) xx.get(1);
		}

		System.out.println(":::::::::::::::::::::::::: " + i);

		Iterator<Tuple> columns = pigServer.openIterator("A");
		
		if (columns.hasNext()) {
			Tuple xy = columns.next();
			colCount = xy.size();
		}
		
		System.out.println("COLUMN::::::::::::::: " + colCount);
	}

}