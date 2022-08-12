package com.gogitek.distributedconsensus.consensus;

import com.gogitek.distributedconsensus.utils.Debug;
import com.gogitek.distributedconsensus.utils.Globals;
import com.gogitek.distributedconsensus.utils.Message;
import com.gogitek.distributedconsensus.utils.Round;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

import static com.gogitek.distributedconsensus.utils.Globals.*;
import static com.gogitek.distributedconsensus.utils.Message.Type.*;

public class Node extends Thread implements Runnable {
    private int rank;
    private int value;
    private int exeSpeed;
    private State stato = State.candidate;
    private boolean decision = false;
    private final Channel channel;
    private final Logger log;
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> nodesAlive     = new ConcurrentSkipListSet<>();  // keep track of the alive nodes
    private Round round;  // current round
    private Round commit;
    private Round lastRound;
    private int lastValue;
    private int proposedValue;
    private long deltaTime = 0;

    Node(@NotNull final Channel channel, int rank, int v) {
        super("Node-" + rank);
        this.log = Logger.getLogger("Node [" + rank + "]");
        this.rank = rank;

        this.value = v;
        this.lastValue = v;
        this.proposedValue = v;

        this.round  = new Round(0, rank);
        this.commit = this.round.copy();
        this.lastRound = this.round.copy();

        this.channel  = channel;
        this.exeSpeed = 1 + generator.nextInt(MAX_EXE_SPEED);
    }

    @Override
    public void run() {
        deltaTime = currentTime();  // take initial execution time

        while (!decision) {
            switch (stato) {
                case voter:
                    voterPhase();
                    advance();
                    break;

                case leader:
                    leaderPhase();
                    break;

                case broken:
                    brokenPhase();
                    break;

                case candidate:
                    electionPhase();
                    break;
            }

            if (isElectionTimeoutExpired()) {
                dlog(Debug.ELECTION_TIMEOUT, round, "[ELECTION TIMEOUT EXPIRED] " + toString());
                logIf(Debug.ELECTION_TIMEOUT, "Election-Timeout", toString());

                deltaTime = currentTime();
                stato = State.candidate;
            }
        }

        logIf(Debug.NODE_STATE, toString());
        dlog(Debug.NODE_STATE, round, "State {%s}", this);
    }

    private void voterPhase() {
        // consuming collect messages
        filterMessages(collect).forEach(msg -> {
            final Round r = msg.getR1();
            final int sender = msg.getSender();

            if (r.greaterEqual(commit)) {
                channel.send(this, sender,
                        new Message(last, r.copy(), lastRound.copy(), lastValue)
                );

                commit = r.copy();
                channel.summary.updateRound(commit);
            } else {
                channel.send(this, sender, new Message(oldRound, r.copy(), commit.copy()));
                dlog(Debug.LOG_OLDROUND, round, "[OLD-ROUND in collect] %s", msg);
            }
        });

        // consuming begin messages
        filterMessages(begin).forEach(msg -> {
            final Round r = msg.getR1();
            final int v   = msg.getValue();
            final int sender = msg.getSender();

            if (r.greaterEqual(commit)) {
                channel.send(this, sender, new Message(accept, round));
                channel.summary.updateRound(r);

                lastRound = r.copy();
                lastValue = v;
            } else {
                channel.send(this, sender, new Message(oldRound, r.copy(), commit.copy()));
                dlog(Debug.LOG_OLDROUND, round, "[OLD-ROUND in begin] %s", msg);
            }
        });
    }

    private void leaderPhase() {
        round = nextRound();
        channel.summary.updateRound(round);

        // -- phase 1
        // -------------------------------------------------
        channel.broadcast(this, new Message(collect, round), true);
        dlog(round, "[Leader-%d] collect", rank);

        // wait a majority of last messages
        long last_timeout = currentTime() + TIMEOUT;
        final Set<Integer> lastCount = new TreeSet<>();
        boolean last_majority = false;

        while (currentTime() < last_timeout) {
            voterPhase();

            if (filterMessages(oldRound).size() > 0) {
                logIf(Debug.LOG_OLDROUND, "Received: old-round in collect");
                dlog(round, "[Leader-%d] received 'old_round' in collect", rank);
                stato = State.voter;
                return;  // lascia il passo
            }

            final List<Message> lastMessages = filterMessages(last);
            lastCount.addAll(Message.uniqueSenders(lastMessages));

            for (Message msg: lastMessages) {
                final Round r = msg.getR1();

                if (r.greaterEqual(lastRound)) {
                    lastRound = r.copy();
                    proposedValue = msg.getValue();
                }
            }

            if (majority(lastCount.size())) {
                last_majority = true;
                break;
            }

            if (advance() == Status.changed)
                return;
        }

        if (!last_majority) {
            logIf(Debug.LOG_TIMEOUT, "TIMEOUT EXPIRED: No [last] majority");
            dlog(Debug.LOG_TIMEOUT, round, "[Leader-%d] TIMEOUT EXPIRED: No [last] majority", rank);
            return;
        }

        channel.broadcast(this, new Message(begin, round, proposedValue), true);
        dlog(round, "[Leader-%d] begin", rank);

        long accept_timeout = currentTime() + TIMEOUT;
        final Set<Integer> acceptCount = new TreeSet<>();

        while (currentTime() < accept_timeout) {
            voterPhase();

            if (filterMessages(oldRound).size() > 0) {
                logIf(Debug.LOG_OLDROUND, "Received: old-round in begin");
                dlog(round, "[Leader-%d] received 'old_round' in begin", rank);
                stato = State.voter;
                return;  // lascia il passo
            }

            final List<Message> acceptMessages = filterMessages(accept);
            acceptCount.addAll(Message.uniqueSenders(acceptMessages));

            if (majority(acceptCount.size())) {
                // there's a decision!
                decision = true;
                value = proposedValue;
                channel.summary.decidedValue(rank, value);
                channel.broadcast(this, new Message(success, value));
                dlog(round, "[Leader-%d] 'success' => %d", rank, value);
                return;  // terminate
            }

            if (advance() == Status.changed)
                return;
        }

        logIf(Debug.LOG_TIMEOUT, String.format("15%d %s", currentTime(), round),
                "TIMEOUT EXPIRED: No [accept] majority");
        dlog(Debug.LOG_TIMEOUT, round,
                "[Leader-%d] TIMEOUT EXPIRED: No [accept] majority", rank);
    }

    private void electionPhase() {
        long timeout = currentTime() + TIMEOUT;

        nodesAlive.clear();
        nodesAlive.add(rank);
        dlog(round, "[Candidate-%d] starts election", rank);

        // try to know the other nodes
        channel.broadcast(this, new Message(queryAlive), true);
        int minRank = rank;

        while (currentTime() < timeout) {
            filterMessages(alive);  // just consume alive messages (the rank is taken while receiving them)

            if (advance() == Status.changed)
                return;
        }

        // find the lowest known rank
        for (Integer id: nodesAlive) {
            if (id < minRank)
                minRank = id;
        }

        // elect the known node with the lowest rank
        stato = (rank == minRank) ? State.leader : State.voter;
        dlog(round, "ELECTION TERMINATED {%s}", this);
    }

    private void brokenPhase() {
        long broken_wait = currentTime() + BROKEN_TIME;

        while (currentTime() < broken_wait)
            delay();

        messageQueue.clear();
        nodesAlive.clear();
        stato = State.candidate;

        dlog(Debug.NODE_REPAIRED, round, "REPAIRED [Node-%d]", rank);
        logIf(Debug.NODE_REPAIRED, "REPAIRED [Node-%d]", rank);

        // TODO: cambiare il valore proposto con uno di default?
        // node memory reset
        lastValue     = value;
        proposedValue = value;
        round  = new Round(0, rank);
        commit = round.copy();
        lastRound = round.copy();
    }

    public void receive(@NotNull Message msg) {
        // receive messages only if not broken
        if (State.broken.equals(stato))
            return;

        logIf(Debug.MSG_RECEPTION, "message received: %s", msg);
        dlog(Debug.MSG_RECEPTION, round, "RECEPTION for [Node-%d] of {%s}", rank, msg);

        // update the known-node-set
        nodesAlive.add(msg.getSender());

        // enqueue the received message
        messageQueue.add(msg);

        // duplication event
        if (msg.getSender() != rank && duplication()) {
            dlog(Debug.MSG_DUPLICATED, round, "DUPLICATION of {%s} from [%d] to [%d]",
                    msg, msg.getSender(), rank
            );

            channel.summary.duplicatedMessages++;
            messageQueue.add(msg);
        }
    }

    private Status advance() {
        delay();

        if (canBroke()) {
            dlog(round, "BROKEN {%s}", this);
            channel.summary.brokenEvents++;
            stato = State.broken;
            return Status.changed;
        }

        final List<Message> successMessages = filterMessages(success);

        for (Message msg: filterMessages(queryAlive)) {
            channel.send(this, msg.getSender(), new Message(alive));
        }

        if (successMessages.size() > 0) {
            int valueDecided = successMessages.get(0).getValue();
            decision = true;
            value = valueDecided;
            channel.summary.decidedValue(rank, value);

            logIf(Debug.NODE_DECISION, "has decided %d", value);
            dlog(round, "[Node-%d-%s] has decided %d", rank, stato, value);

            channel.broadcast(this, new Message(success, value));

            return Status.changed;
        }

        logIf(Debug.NODE_STATE, this.toString());
        dlog(Debug.NODE_STATE, round, toString());

        return Status.alive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return rank == node.rank;
    }

    public int getRank() {

        return rank;
    }

    public int getValue() {
        return value;
    }

    public Round getRound() {
        return round;
    }

    enum State {
        leader,
        voter,
        broken,
        candidate,
    }

    enum Status {
        alive,
        //        dead,
        changed,
    }

    @Override
    public String toString() {
        return String.format("Node-%d [%s, round: %s, commit: %s, value: %d, known_nodes: %s]",
                rank, stato, round, commit, proposedValue, nodesAlive);
    }

    private void delay() {
        try { sleep(exeSpeed); } catch (InterruptedException ignored) { }
    }

    private long currentTime() {
        return java.lang.System.currentTimeMillis();
    }

    private boolean majority(int amount) {
        return (amount >= (nodesAlive.size() + 1) / 2);
    }

    private boolean canBroke() {
        return Globals.BROKEN_RATE >= 1 + generator.nextInt(1000 * Globals.MAX_EXE_SPEED);
    }

    private boolean isElectionTimeoutExpired() {
        return (currentTime() - deltaTime > Globals.ELECTION_TIMEOUT);
    }

    private boolean duplication() {
        int guess = 1 + generator.nextInt(100);
        return guess <= Globals.MESSAGE_DUPLICATION_RATE;
    }

    private List<Message> filterMessages(Message.Type type) {
        List<Message> selected = new ArrayList<>();

        for (Message message: messageQueue) {
            if (type.equals(message.getType())) {
                selected.add(message);
                messageQueue.remove(message);
            }
        }

        return selected;
    }

    private Round nextRound() {
        if (lastRound.greaterEqual(round))
            return new Round(lastRound.getCount() + 1, this.rank);
        else
            return round.increase();
    }

    private void log(final String format, Object...args) {
        if (Debug.CONSOLE_LOG)
            log.warning("[" + rank + "] " + String.format(format, args));
    }

    private void logIf(boolean flag, final String format, Object...args) {
        if (flag || Debug.LOG_ALL)
            log(format, args);
    }

    private void dlog(final String key, final String format, Object...args) {
        Debug.log(key, format, args);
    }

    private void dlog(final Round round, final String format, Object...args) {
        final String key = String.format("15%d %s", currentTime(), round);
        Debug.log(key, format, args);
    }

    private void dlog(boolean flag, final String key, final String format, Object...args) {
        Debug.logIf(flag, key, format, args);
    }

    private void dlog(boolean flag, final Round round, final String format, Object...args) {
        final String key = String.format("15%d %s", currentTime(), round);
        Debug.logIf(flag, key, format, args);
    }

    private static final Random generator = new Random();
}
