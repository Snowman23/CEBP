package ro.upb.cebp.workermanager.scheduler;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.topics.KafkaTopics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Consumes normalized events and emits route/source-coherent batches on size or a 5-second window. */
@Component
public class RawEventWindowBatcher {
    private static final int BATCH_SIZE = 25;
    private final JobDispatcher dispatcher;
    private final Map<String, List<NormalizedTransitEvent>> buffers = new LinkedHashMap<>();

    public RawEventWindowBatcher(JobDispatcher dispatcher) { this.dispatcher = dispatcher; }

    @KafkaListener(topics = KafkaTopics.TRANSIT_EVENTS_RAW, groupId = "worker-manager-raw-group")
    public void onRawTransitEvent(NormalizedTransitEvent event) { accept(event); }

    public void accept(NormalizedTransitEvent event) {
        if (event == null) return;
        synchronized (buffers) {
            String source = event.source() == null ? "UNKNOWN" : event.source().name();
            String route = event.routeId() == null ? "DEFAULT" : event.routeId();
            String key = source + ":" + route;
            List<NormalizedTransitEvent> buffer = buffers.computeIfAbsent(key, ignored -> new ArrayList<>());
            buffer.add(event);
            if (buffer.size() >= BATCH_SIZE) dispatch(key, buffer);
        }
    }

    @Scheduled(fixedDelay = 5000)
    public void flushWindow() {
        synchronized (buffers) {
            for (String key : new ArrayList<>(buffers.keySet())) {
                List<NormalizedTransitEvent> buffer = buffers.get(key);
                if (buffer != null && !buffer.isEmpty()) dispatch(key, buffer);
            }
        }
    }

    private void dispatch(String key, List<NormalizedTransitEvent> buffer) {
        List<NormalizedTransitEvent> batch = List.copyOf(buffer);
        buffer.clear();
        NormalizedTransitEvent first = batch.getFirst();
        dispatcher.createAndDispatch(first.source(), first.routeId(), batch);
    }
}
