package org.popcraft.chunky.command;

import org.popcraft.chunky.Chunky;
import org.popcraft.chunky.platform.Player;
import org.popcraft.chunky.platform.Sender;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ActionbarCommand implements ChunkyCommand {
    private final Chunky chunky;

    public ActionbarCommand(final Chunky chunky) {
        this.chunky = chunky;
    }

    @Override
    public void execute(final Sender sender, final CommandArguments arguments) {
        if (!sender.isPlayer()) {
            return;
        }
        final Player player = (Player) sender;
        final UUID uuid = player.getUUID();
        final Set<UUID> actionBarPlayers = chunky.getActionBarPlayers();
        if (actionBarPlayers.contains(uuid)) {
            actionBarPlayers.remove(uuid);
            sender.sendMessagePrefixed("Actionbar disabled.");
        } else {
            actionBarPlayers.add(uuid);
            sender.sendMessagePrefixed("Actionbar enabled.");
        }
        chunky.saveActionBarPlayers();
    }

    @Override
    public List<String> suggestions(final CommandArguments arguments) {
        return List.of();
    }
}

