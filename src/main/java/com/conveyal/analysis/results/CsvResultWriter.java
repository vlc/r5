package com.conveyal.analysis.results;

import com.conveyal.file.FileUtils;
import com.csvreader.CsvWriter;
import com.google.common.base.Preconditions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Write regional analysis results arriving from workers into a CSV file. This is the output format
 * for origin/destination "skim" matrices, or for accessibility indicators from non-gridded
 * ("freeform") origin point sets.
 */
public class CsvResultWriter {
    public final File bufferFile = FileUtils.createScratchFile();
    private final CsvWriter csvWriter = new CsvWriter(new BufferedWriter(new FileWriter(bufferFile)), ',');
    private final int nDataColumns;

    /**
     * Construct a writer to record incoming results in a CSV file.
     */
    CsvResultWriter (String... columns) throws IOException {
        this.nDataColumns = columns.length;
        csvWriter.writeRecord(columns);
    }

    /**
     * Write a single row into the CSV file.
     */
    synchronized void writeOneRow(String... values) throws IOException {
        // CsvWriter is not threadsafe and multiple threads may call this, so the actual writing is synchronized (TODO confirm)
        Preconditions.checkArgument(
                values.length == nDataColumns,
                "Attempted to write the wrong number of columns to a result CSV"
        );
        synchronized (this) {
            csvWriter.writeRecord(values);
        }
    }

    synchronized void terminate () {
        csvWriter.close();
        bufferFile.delete();
    }
}
