package com.gogitek.distributedconsensus;

import com.gogitek.distributedconsensus.consensus.Paxos;
import com.gogitek.distributedconsensus.start.AverageSummary;
import com.gogitek.distributedconsensus.utils.Debug;
import com.gogitek.distributedconsensus.utils.Globals;
import com.gogitek.distributedconsensus.utils.Logger;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Locale;

@RestController
@RequestMapping("/test")
@AllArgsConstructor
public class MainController {
    Paxos paxos;

    @GetMapping()
    public ResponseEntity<?> resolve(@RequestParam(defaultValue = "") String input){
        Logger.setLevel(Logger.DEBUG);
        Globals.CHANNEL_DELAY     = 100;
        Globals.TIMEOUT           = (Globals.CHANNEL_DELAY * 3);
        Globals.MESSAGE_LOST_RATE = 35+5;
        Globals.BROKEN_RATE       = 10;
        Globals.MESSAGE_DUPLICATION_RATE = 15;
        Globals.MAX_EXE_SPEED     = 10;
        Globals.BROKEN_TIME       = Globals.CHANNEL_DELAY * 4;
        Globals.ELECTION_TIMEOUT  = Globals.TIMEOUT + Globals.BROKEN_TIME;

        Debug.CONSOLE_LOG = false;
        Debug.MSG_RECEPTION = true;
        Debug.LOG_OLDROUND  = true;
        Debug.ELECTION_TIMEOUT = true;

        paxos.prompt("Number of simulations: ", null, in -> {
            int num = 1;
            try { num = Integer.parseInt(input); } catch (RuntimeException ignored) {}

            new AverageSummary(num, 1, 2, 0, 3)
                    .calculate()
                    .print();

            paxos.prompt("\nShow the executions log? (y/n)", "y",
                    x -> Debug.printExecutionsLog(), input.trim().toLowerCase());
        }, input.trim().toLowerCase());

        System.exit(0);
        return ResponseEntity.ok("Done, check log in console");
    }
}
