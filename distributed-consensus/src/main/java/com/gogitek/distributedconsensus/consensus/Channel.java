package com.gogitek.distributedconsensus.consensus;

import com.gogitek.distributedconsensus.start.Summary;
import com.gogitek.distributedconsensus.utils.Debug;
import com.gogitek.distributedconsensus.utils.Globals;
import com.gogitek.distributedconsensus.utils.Message;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.gogitek.distributedconsensus.utils.Globals.MESSAGE_LOST_RATE;

public class Channel {
    private final Logger log = Logger.getLogger("Channel");
    private final List<Node> nodes = new ArrayList<>();
    public  final Summary summary  = new Summary();


    public Channel(@NotNull int... values) {
        int numNodes = values.length;
        summary.totalNodes = numNodes;

        // creating nodes
        for (int rank = 0; rank < numNodes; ++rank) {
            nodes.add(new Node(this, rank, values[rank]));
        }
    }

    public Channel launch() {
        summary.startTime();  // take the initial time

        for (Node node: nodes)
            node.start();

        return this;
    }

    public void onTermination(@NotNull Consumer<Channel> callback) {
        for (Node node: nodes) {
            try { node.join(); } catch (InterruptedException ignored) { }
        }

        callback.accept(this);
    }

    public void send(@NotNull final Node from, int to, @NotNull final Message message) {
        assert to < nodes.size();

        new SenderThread(from, to, message.copy())
                .start();
    }

    public void broadcast(@NotNull final Node from, @NotNull final Message message, boolean sendToMe) {
        for (Node node: nodes) {
            if (!sendToMe && from.equals(node))
                continue;

            send(from, node.getRank(), message);
        }
    }

    public void broadcast(@NotNull final Node from, @NotNull final Message message) {
        broadcast(from, message, false);
    }

    // -----------------------------------------------------------------------------------------------------------------

    private class SenderThread extends Thread {
        private final Node from;
        private final int to;
        private final Message message;

        SenderThread(final Node from, final int to, final Message message) {
            this.from = from;
            this.to = to;
            this.message = message;
            this.message.setSender(from.getRank());

            Debug.logIf(Debug.MSG_SENDING, String.format("15%d %s", System.currentTimeMillis(), from.getRound()),
                    "SENDING of {%s} from [%d] to [%d]", message, from.getRank(), to);
            logIf(Debug.MSG_SENDING, "SENDING of {%s} from [%d] to [%d]", message, from.getRank(), to);
            summary.totalMessages++;
        }

        @Override
        public void run() {
            final Node receiver = nodes.get(to);

            if (from.getRank() != receiver.getRank()) {
                if (channelError()) {
                    summary.lostMessages++;
                    logIf(Debug.MSG_LOST, "LOST of {%s} from [%d] to [%d]", message, from.getRank(), to);
                    Debug.log(String.format("15%d %s", System.currentTimeMillis(), from.getRound()),
                            "LOST of {%s} from [%d] to [%d]", message, from.getRank(), to);
                    return;
                }

                sendDelay();
            }

            receiver.receive(message);
        }

        private void sendDelay() {
            final Random generator = new Random();
            try { sleep(generator.nextInt(1 + Globals.CHANNEL_DELAY)); } catch (InterruptedException ignored) { }
        }
    }

    private boolean channelError() {
        final Random generator = new Random();
        int guess = 1 + generator.nextInt(100);

        return guess <= MESSAGE_LOST_RATE;
    }

    private void logIf(boolean flag, final String format, Object...args) {
        if (Debug.CONSOLE_LOG && (flag || Debug.LOG_ALL))
            log.warning(String.format(format, args));
    }
}

