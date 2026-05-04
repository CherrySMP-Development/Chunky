package org.popcraft.chunky.command;

import org.popcraft.chunky.Chunky;
import org.popcraft.chunky.platform.Sender;
import org.popcraft.chunky.util.Input;
import org.popcraft.chunky.util.TranslationKey;

import java.util.List;
import java.util.Optional;

public class SpeedCommand implements ChunkyCommand {
    private static final int MIN_CPS = 100;
    private static final int MAX_CPS = 10000;
    private final Chunky chunky;

    public SpeedCommand(final Chunky chunky) {
        this.chunky = chunky;
    }

    @Override
    public void execute(final Sender sender, final CommandArguments arguments) {
        if (arguments.size() < 1) {
            sender.sendMessage(TranslationKey.HELP_SPEED);
            return;
        }
        final Optional<Integer> cps = arguments.next().flatMap(Input::tryInteger).filter(value -> value >= MIN_CPS && value <= MAX_CPS);
        if (cps.isEmpty()) {
            sender.sendMessage(TranslationKey.HELP_SPEED);
            return;
        }
        chunky.setSpeed(cps.get());
        sender.sendMessagePrefixed(TranslationKey.FORMAT_SPEED, cps.get());
    }

    @Override
    public List<String> suggestions(final CommandArguments arguments) {
        if (arguments.size() == 1) {
            return List.of("100", "150", "200", "300", "500", "1000", "2000", "3000", "5000", "7500", "10000");
        }
        return List.of();
    }
}
