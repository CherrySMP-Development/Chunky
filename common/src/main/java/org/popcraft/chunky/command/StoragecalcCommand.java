package org.popcraft.chunky.command;

import org.popcraft.chunky.Chunky;
import org.popcraft.chunky.Selection;
import org.popcraft.chunky.iterator.ChunkIteratorFactory;
import org.popcraft.chunky.platform.Sender;
import org.popcraft.chunky.util.Disk;
import org.popcraft.chunky.util.Formatting;

import java.util.List;

public class StoragecalcCommand implements ChunkyCommand {
    private final Chunky chunky;

    public StoragecalcCommand(final Chunky chunky) {
        this.chunky = chunky;
    }

    @Override
    public void execute(final Sender sender, final CommandArguments arguments) {
        final Selection selection = chunky.getSelection().build();
        final long totalChunks = ChunkIteratorFactory.getChunkIterator(selection).total();
        final long estimatedBytes = Disk.estimatedSpace(selection);

        sender.sendMessagePrefixed("Estimated space for selection in " + selection.world().getName() + " (" + totalChunks + " chunks): " + Formatting.bytes(estimatedBytes));
    }

    @Override
    public List<String> suggestions(final CommandArguments arguments) {
        return List.of();
    }
}

