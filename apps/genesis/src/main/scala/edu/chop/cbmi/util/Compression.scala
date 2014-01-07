package edu.chop.cbmi.util

import java.io._
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import java.util.zip.GZIPOutputStream
import org.apache.commons.compress.utils.IOUtils

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/26/13
 * Time: 12:00 PM
 */
object Compression {

  def compressFile(file: File, output: File) = compressFiles(List(file), output)


  def compressFiles(files: Iterable[File], output: File) = {
    // Create the output stream for the output file
    val fos = new FileOutputStream(output);
    // Wrap the output file stream in streams that will tar and gzip everything
    val taos = new TarArchiveOutputStream(
      new GZIPOutputStream(new BufferedOutputStream(fos)));
    // TAR has an 8 gig file limit by default, this gets around that
    taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR); // to get past the 8 gig limit
    // TAR originally didn't support long file names, so enable the support for it
    taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

    // Get to putting all the files in the compressed output file
    files.foreach {
      f => addFilesToCompression(taos, f, ".")
    }

    // Close everything up
    taos.close()
    fos.close()
  }


  def addFilesToCompression(taos: TarArchiveOutputStream, file: File, dir: String): Unit = {
    // Create an entry for the file
    taos.putArchiveEntry(new TarArchiveEntry(file, s"$dir/${file.getName}"))
    if (file.isFile) {
      // Add the file to the archive
      val bis = new BufferedInputStream(new FileInputStream(file));
      IOUtils.copy(bis, taos);
      taos.closeArchiveEntry();
      bis.close();
    }
    else if (file.isDirectory()) {
      // close the archive entry
      taos.closeArchiveEntry();
      // go through all the files in the directory and using recursion, add them to the archive
      file.listFiles().foreach {
        child => addFilesToCompression(taos, child, s"$dir/${file.getName}")
      }
    }
  }
}
