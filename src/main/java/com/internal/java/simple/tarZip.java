package com.internal.java.simple;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

public class tarZip {
	public static void main(String[] args) throws IOException {
		String inDir = "D:\\TEST";
		String outDir = "D:\\TESTOUT";

		String outPath = tar(inDir, outDir);
		zip(outPath, outDir);
		bzip2(outPath, outDir);
	}
	
	private static void bzip2(String inPath, String outDir) throws IOException {
		File f = new File(inPath);

		String gzipFile = outDir + "\\" + f.getName() + ".bz2";
		
		FileInputStream fis = new FileInputStream(inPath);
		FileOutputStream fos = new FileOutputStream(gzipFile);
		BZip2CompressorOutputStream bz2 = new BZip2CompressorOutputStream(fos);
		byte[] buffer = new byte[1024];
		int len;
		while ((len = fis.read(buffer)) != -1) {
			bz2.write(buffer, 0, len);
		}
		// close resources
		bz2.close();
		fos.close();
		fis.close();
		
		System.out.println("zipped!!!");
	}

	private static void zip(String inPath, String outDir) throws IOException {
		File f = new File(inPath);

		String gzipFile = outDir + "\\" + f.getName() + ".gz";
		
		FileInputStream fis = new FileInputStream(inPath);
		FileOutputStream fos = new FileOutputStream(gzipFile);
//		BZip2CompressorOutputStream bz2 = new BZip2CompressorOutputStream(fos);
		GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
		byte[] buffer = new byte[1024];
		int len;
		while ((len = fis.read(buffer)) != -1) {
			gzipOS.write(buffer, 0, len);
		}
		// close resources
		gzipOS.close();
		fos.close();
		fis.close();

		System.out.println("zipped!!!");
	}

	private static String tar(String inDir, String outDir)
			throws IOException {
		String outPath = outDir + "\\test.tar";

		System.out.println(outPath + " to be created now.");

		// Output file stream
		FileOutputStream dest = new FileOutputStream(outPath);

		// Create a TarOutputStream
		TarArchiveOutputStream out = new TarArchiveOutputStream(new BufferedOutputStream(dest));
		
		
		final File folder = new File(inDir);
		
		// Files to tar
		File[] filesToTar = folder.listFiles();

		for (File f : filesToTar) {
			
			System.out.println("HERE FILES:: " + f.getName());
			
			out.putArchiveEntry(out.createArchiveEntry(f, f.getName()));
			BufferedInputStream origin = new BufferedInputStream(new FileInputStream(f));

			int count;
			byte data[] = new byte[2048];
			while ((count = origin.read(data)) != -1) {
				out.write(data, 0, count);
			}
			out.closeArchiveEntry();
			out.flush();
			origin.close();
		}
		out.close();

		System.out.println("tar created!!!");

		return outPath;
	}
}