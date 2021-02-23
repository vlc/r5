package com.conveyal.analysis.results;

import com.conveyal.analysis.models.RegionalAnalysis;
import com.conveyal.analysis.models.RegionalAnalysis.Result;
import com.conveyal.file.FileUtils;
import com.conveyal.r5.analyst.cluster.PathResult;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.RegionalWorkResult;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * This assembles regional results arriving from workers into one or more files per regional analysis on
 * the backend. This is not a singleton component: one MultiOriginAssembler instance is created per currently active
 * job awaiting results from workers. It delegates to ResultWriters to actually slot results into different file formats.
 */
public class MultiOriginAssembler {
    public static final Logger LOG = LoggerFactory.getLogger(MultiOriginAssembler.class);

    private final BiConsumer<String, File> moveIntoStorage;

    /**
     * The regional analysis for which this object is assembling results.
     * We retain the whole object rather than just its ID so we'll have the full details, e.g. destination point set
     * IDs and scenario, things that are stripped out of the template task sent to the workers.
     */
    private final RegionalAnalysis regionalAnalysis;
    private final RegionalTask task;

    /**
     * We create one GridResultWriter for each destination pointset and percentile.  Each of those output files
     * contains data for all travel time cutoffs at each origin.
     */
    private GridResultWriter[][] accessibilityGridWriters;

    /**
     * If the job includes a freeform origin pointset, csv results may be written for each of those freeform origins.
     * Additionally, if templateTask.recordAccessibility = true, accessibility results will be written to CSV instead
     * of the grids above.
     */
    private final Map<Result, CsvResultWriter> csvWriters = new HashMap<>();

    /** TODO check if error is true before all results are received (when receiving each result?) and cancel job. */
    private boolean error = false;

    /**
     * The number of distinct origin points for which we've received at least one result. If for
     * whatever reason we receive two or more results for the same origin this should only be
     * incremented once). It's incremented in a synchronized block to avoid race conditions.
     */
    public int nComplete = 0;

    /**
     * We need to keep track of which specific origins are completed, to avoid double counting if we
     * receive more than one result for the same origin. As with nComplete, access must be
     * synchronized to avoid race conditions. The nComplete field could be derived from this BitSet,
     * but nComplete can be read in constant time whereas counting true bits in a BitSet takes
     * linear time.
     * FIXME it doesn't seem like both the Job and the MultiOriginAssembler should be tracking job progress.
     *       Might be preferable to track this only in the job, and have it close the assembler when the job finishes.
     */
    private final BitSet originsReceived;

    /**
     * Total number of origin points for which we're expecting results. Note that the total
     * number of results received could be higher in the event of an overzealous task redelivery.
     */
    public final int nOriginsTotal;

    /** The number of different percentiles for which we're calculating accessibility on the workers. */
    private final int nPercentiles;

    /** The number of destination point sets to which we're calculating accessibility */
    private final int nDestinationPointSets;

    /**
     * The number of different travel time cutoffs being applied when computing accessibility for each origin. This
     * is the number of values stored per origin cell in an accessibility results grid.
     * Note that we're storing only the number of different cutoffs, but not the cutoff values themselves in the file.
     * This means that the files can only be properly interpreted with the Mongo metadata from the regional analysis.
     * This is an intentional choice to avoid changing the file format, and in any case these files are not expected
     * to ever be used separately from an environment where the Mongo database is available.
     */
    private final int nCutoffs;

    /**
     * Constructor. This sets up one or more ResultWriters depending on whether we're writing gridded or non-gridded
     * cumulative opportunities accessibility, or origin-destination travel times.
     */
    public MultiOriginAssembler (RegionalAnalysis regionalAnalysis, RegionalTask task, BiConsumer<String, File> moveIntoStorage) {
        this.moveIntoStorage = moveIntoStorage;
        this.regionalAnalysis = regionalAnalysis;
        this.task = task;
        nPercentiles = task.percentiles.length;
        // Newly launched analyses have the cutoffs field, even when being sent to old workers that don't read it.
        nCutoffs = task.cutoffsMinutes.length;
        nDestinationPointSets = task.makeTauiSite ? 0 : task.destinationPointSetKeys.length;
        nOriginsTotal = task.nTasksTotal;
        originsReceived = new BitSet(task.nTasksTotal);

        try {
            if (task.recordAccessibility) {
                if (task.originPointSet != null) {
                    LOG.info(
                        "Creating CSV file to store accessibility results for {} origins.",
                        task.nTasksTotal
                    );
                    csvWriters.put(Result.ACCESS, new CsvResultWriter( "origin", "destination", "access"));
                } else {
                    // Create one grid writer per percentile and destination pointset
                    accessibilityGridWriters = new GridResultWriter[nDestinationPointSets][nPercentiles];
                    for (int d = 0; d < nDestinationPointSets; d++) {
                        for (int p = 0; p < nPercentiles; p++) {
                            accessibilityGridWriters[d][p] = new GridResultWriter(task);
                        }
                    }
                }
            }

            if (task.recordTimes) {
                LOG.info("Creating csv file to store time results for {} origins.", task.nTasksTotal);
                csvWriters.put(Result.TIMES, new CsvResultWriter("origin", "destination", "time"));
            }

            if (task.includePathResults) {
                LOG.info("Creating csv file to store path results for {} origins.", task.nTasksTotal);
                String[] pathColumns = ArrayUtils.addAll(new String[]{"origin", "destination"}, PathResult.DATA_COLUMNS);
                csvWriters.put(Result.PATHS, new CsvResultWriter(pathColumns));
            }
        } catch (IOException e) {
            error = true;
            LOG.error("Exception while creating multi-origin assembler: " + e.toString());
        }
    }

    /**
     * Gzip the output files and persist them to cloud storage.
     */
    private synchronized void gzipOutputFilesAndStore() {
        LOG.info("Finished receiving data for multi-origin analysis {}", task.jobId);
        try {
            if (accessibilityGridWriters != null) {
                for (int d = 0; d < nDestinationPointSets; d++) {
                    for (int p = 0; p < nPercentiles; p++) {
                        int percentile = task.percentiles[p];
                        String destinationPointSetId = regionalAnalysis.destinationPointSetIds[d];
                        String gridFileName =
                                String.format("%s_%s_P%d.access", task.jobId, destinationPointSetId, percentile);
                        File gzipped = FileUtils.gzip(accessibilityGridWriters[d][p].bufferFile);
                        moveIntoStorage.accept(gridFileName, gzipped);
                    }
                }
            }

            for (Map.Entry<Result, CsvResultWriter> entry : csvWriters.entrySet()) {
                String storagePath = regionalAnalysis.getCsvStoragePath(entry.getKey());
                File gzipped = FileUtils.gzip(entry.getValue().bufferFile);
                moveIntoStorage.accept(storagePath, gzipped);
            }

            terminate();
        } catch (Exception e) {
            LOG.error("Error uploading results of multi-origin analysis {}", task.jobId, e);
        }
    }

    /**
     * There is a bit of logic in this method that wouldn't strictly need to be synchronized (the
     * dimension checks) but those should take a trivial amount of time. For safety and simplicity
     * we will synchronize the whole method. The downside is that this prevents one thread from
     * writing accessibility while another was writing travel time CSV, but this should not be
     * assumed to have any impact on performance unless measured. The writeOneValue methods are also
     * synchronized for good measure. There should be no cost to retaining the lock.
     */
    public synchronized void handleMessage (RegionalWorkResult workResult) {
        try {
            if (task.recordAccessibility) {
                // Sanity check the shape of the work result we received against expectations.
                checkAccessibilityDimension(workResult);
                // Infer x and y cell indexes based on the template task
                int taskNumber = workResult.taskId;
                // Drop work results for this particular origin into a little-endian output file.
                // TODO more efficient way to write little-endian integers
                // TODO check monotonic increasing invariants here rather than in worker.
                for (int d = 0; d < workResult.accessibilityValues.length; d++) {
                    int[][] percentilesForGrid = workResult.accessibilityValues[d];
                    if (csvWriters.containsKey(Result.ACCESS)) {
                        String originId = task.originPointSet.getId(workResult.taskId);
                        // FIXME this is writing only accessibility for the first percentile and cutoff
                        csvWriters.get(Result.ACCESS).writeOneRow(originId, "", String.valueOf(percentilesForGrid[0][0]));
                    } else {
                        for (int p = 0; p < nPercentiles; p++) {
                            int[] cutoffsForPercentile = percentilesForGrid[p];
                            GridResultWriter writer = accessibilityGridWriters[d][p];
                            writer.writeOneOrigin(taskNumber, cutoffsForPercentile);
                        }
                    }
                }
            }

            if (task.recordTimes) {
                // Sanity check the shape of the work result we received against expectations.
                checkTravelTimeDimension(workResult);
                String originId = task.originPointSet.getId(workResult.taskId);
                for (int p = 0; p < nPercentiles; p++) {
                    int[] percentileResult = workResult.travelTimeValues[p];
                    for (int d = 0; d < percentileResult.length; d++) {
                        int travelTime = percentileResult[d];
                        String destinationId = destinationId(workResult.taskId, d);
                        csvWriters.get(Result.TIMES).writeOneRow(originId, destinationId, String.valueOf(travelTime));
                    }
                }
            }

            if (task.includePathResults) {
                checkPathDimension(workResult);
                ArrayList<String[]>[] pathsToPoints = workResult.pathResult;
                for (int d = 0; d < pathsToPoints.length; d++) {
                    ArrayList<String[]> pathsIterations = pathsToPoints[d];
                    for (String[] iterationDetails : pathsIterations) {
                        String originId = task.originPointSet.getId(workResult.taskId);
                        String destinationId = destinationId(workResult.taskId, d);
                        checkDimension(workResult, "columns", iterationDetails.length, PathResult.DATA_COLUMNS.length);
                        csvWriters.get(Result.PATHS).writeOneRow(ArrayUtils.addAll(new String[]{originId, destinationId}, iterationDetails));
                    }
                }
            }

            // Don't double-count origins if we receive them more than once. Atomic get-and-increment requires
            // synchronization, currently achieved by synchronizing this entire method.
            if (!originsReceived.get(workResult.taskId)) {
                originsReceived.set(workResult.taskId);
                nComplete += 1;
            }
            if (nComplete == nOriginsTotal && !error) {
                gzipOutputFilesAndStore();
                // TODO Persist to the database
                this.regionalAnalysis.complete = true;
            }
        } catch (Exception e) {
            error = true;
            LOG.error("Error assembling results for query {}", task.jobId, e);
        }
    }

    /**
     * Check that each dimension of the 3D results array matches the expected size for the job being processed.
     * There are different dimension requirements for accessibility, travel time, and path results, so three different
     * dimension check methods.
     */
    private void checkAccessibilityDimension (RegionalWorkResult workResult) {
        checkDimension(workResult, "destination pointsets", workResult.accessibilityValues.length, this.nDestinationPointSets);
        for (int[][] percentilesForGrid : workResult.accessibilityValues) {
            checkDimension(workResult, "percentiles", percentilesForGrid.length, this.nPercentiles);
            for (int[] cutoffsForPercentile : percentilesForGrid) {
                checkDimension(workResult, "cutoffs", cutoffsForPercentile.length, this.nCutoffs);
            }
        }
    }

    /**
     * Check that each dimension of the 2D results array matches the expected size for the job being processed.
     * There are different dimension requirements for accessibility and travel time results, so two different methods.
     */
    private void checkTravelTimeDimension (RegionalWorkResult workResult) {
        // In one-to-one mode, we expect only one value per origin, the destination point at the same pointset index as
        // the origin point. Otherwise, for each origin, we expect one value per destination.
        final int nDestinations = task.oneToOne ? 1 : task.destinationPointSets[0].featureCount();
        checkDimension(workResult, "percentiles", workResult.travelTimeValues.length, nPercentiles);
        for (int[] percentileResult : workResult.travelTimeValues) {
            checkDimension(workResult, "destinations", percentileResult.length, nDestinations);
        }
    }

    private void checkPathDimension (RegionalWorkResult workResult) {
        // In one-to-one mode, we expect only one value per origin, the destination point at the same pointset index as
        // the origin point. Otherwise, for each origin, we expect one value per destination.
        final int nDestinations = task.oneToOne ? 1 : task.destinationPointSets[0].featureCount();
        checkDimension(workResult, "destinations", workResult.pathResult.length, nDestinations);
    }

    /** Clean up and cancel this grid assembler, typically when a job is canceled while still being processed. */
    public synchronized void terminate () throws IOException {
        if (accessibilityGridWriters != null) {
            for (GridResultWriter[] writers : accessibilityGridWriters) {
                for (GridResultWriter writer : writers) {
                    writer.terminate();
                }
            }
        }

        for (CsvResultWriter writer : csvWriters.values()) {
           writer.terminate();
        }
    }

    String destinationId(int taskId, int index) {
        // oneToOne results will have the same origin and destination IDs.
        // Always writing both should alert the user if something is amiss.
        if (task.oneToOne) {
            return task.destinationPointSets[0].getId(taskId);
        } else {
            return task.destinationPointSets[0].getId(index);
        }
    }

    /**
     * We don't have any straightforward way to return partial CSV results, so we only return
     * partially filled grids. This leaks the file object out of the abstraction so is not ideal,
     * but will work for now to allow partial display.
     */
    public File getGridBufferFile () {
        if (accessibilityGridWriters == null) {
            return null;
        } else {
            // TODO this returns only one buffer file, which has not been processed by the SelectingGridReducer
            return accessibilityGridWriters[0][0].bufferFile;
        }
    }

    /**
     * Validate that the work results we're receiving match what is expected for the job at hand.
     */
    private void checkDimension (RegionalWorkResult workResult, String dimensionName,
                                 int seen, int expected) {
        if (seen != expected) {
            LOG.error("Result for task {} of job {} has {} {}, expected {}.",
                    workResult.taskId, workResult.jobId, dimensionName, seen, expected);
            error = true;
        }
    }

}
