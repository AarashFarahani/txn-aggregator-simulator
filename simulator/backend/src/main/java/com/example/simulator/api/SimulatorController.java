package com.example.simulator.api;
import com.example.simulator.model.SimulationRequest;
import com.example.simulator.service.SimulationService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
@RestController @RequestMapping("/api/simulator") @CrossOrigin(origins="*")
public class SimulatorController {
 private final SimulationService service;
 public SimulatorController(SimulationService service){this.service=service;}
 @PostMapping("/start") public Map<String,String> start(@Valid @RequestBody SimulationRequest r){service.start(r);return Map.of("message","started");}
 @PostMapping("/pause") public Map<String,String> pause(){service.pause();return Map.of("state","PAUSED");}
 @PostMapping("/resume") public Map<String,String> resume(){service.resume();return Map.of("state","RUNNING");}
 @PostMapping("/stop") public Map<String,String> stop(){service.stop();return Map.of("state","STOPPED");}
 @GetMapping("/status") public Map<String,Object> status(){return service.status();}
}
