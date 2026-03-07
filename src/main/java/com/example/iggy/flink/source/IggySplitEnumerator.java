package com.example.iggy.flink.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * Enumerator for the Iggy source that discovers and assigns splits to readers.
 *
 * <p>For Iggy, each split represents one stream/topic/partition combination.
 * The enumerator assigns splits to reader subtasks on request. Since Iggy topics
 * are unbounded (continuous), this enumerator manages a single persistent split
 * per configured partition.
 *
 * <p>The enumerator state (unassigned splits queue) is checkpointed by Flink so
 * that on recovery, splits previously assigned to failed readers can be reassigned.
 */
public class IggySplitEnumerator
        implements SplitEnumerator<IggySplit, Collection<IggySplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySplitEnumerator.class);

    private final SplitEnumeratorContext<IggySplit> context;
    private final Queue<IggySplit> unassignedSplits;

    public IggySplitEnumerator(
            SplitEnumeratorContext<IggySplit> context,
            Collection<IggySplit> initialSplits) {
        this.context = context;
        this.unassignedSplits = new ArrayDeque<>(initialSplits);
    }

    @Override
    public void start() {
        LOG.info("IggySplitEnumerator started with {} unassigned splits", unassignedSplits.size());
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        IggySplit split = unassignedSplits.poll();
        if (split != null) {
            LOG.info("Assigning split {} to subtask {}", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            // No splits available — for an unbounded source we don't signal end-of-input
            LOG.debug("No splits available for subtask {}, not sending end-of-input (unbounded source)", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<IggySplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        unassignedSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // When a new reader comes up, immediately try to give it a split
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<IggySplit> snapshotState(long checkpointId) {
        return new ArrayList<>(unassignedSplits);
    }

    @Override
    public void close() {
        LOG.info("IggySplitEnumerator closed");
    }
}
