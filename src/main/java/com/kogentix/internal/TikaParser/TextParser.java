package com.kogentix.internal.TikaParser;

import java.io.*;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.txt.TXTParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class TextParser {
	public static void main(final String[] args) throws IOException,
			SAXException, TikaException {

		System.out.println("Hi .............");
		
		// detecting the file type
		BodyContentHandler handler = new BodyContentHandler();
		Metadata metadata = new Metadata();
		FileInputStream inputstream = new FileInputStream(new File("C:\\Users\\RAKTOTPAL\\Desktop\\TICKET HYD-GHY\\NF7202055814469.Invoice.pdf"));
		
		ParseContext pcontext = new ParseContext();

		// Text document parser
		TXTParser TexTParser = new TXTParser();
		TexTParser.parse(inputstream, handler, metadata, pcontext);
		System.out.println("Contents of the document:" + handler.toString());
		System.out.println("Metadata of the document:");
		String[] metadataNames = metadata.names();

		for (String name : metadataNames) {
			System.out.println(name + " : " + metadata.get(name));
		}
	}
}
