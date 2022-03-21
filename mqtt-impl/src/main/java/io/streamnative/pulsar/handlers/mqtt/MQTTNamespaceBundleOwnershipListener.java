package io.streamnative.pulsar.handlers.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MQTTNamespaceBundleOwnershipListener implements NamespaceBundleOwnershipListener {

    private final NamespaceService namespaceService;

    private final List<MQTTTopicOwnershipListener> listeners = new ArrayList<>();

    public MQTTNamespaceBundleOwnershipListener(NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
        this.namespaceService.addNamespaceBundleOwnershipListener(this);
    }

    public void addListener(MQTTTopicOwnershipListener listener) {
        listeners.add(listener);
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        //
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        namespaceService.getFullListOfTopics(bundle.getNamespaceObject())
                .thenApply(topics -> topics.stream()
                        .filter(topic -> bundle.includes(TopicName.get(topic)))
                        .collect(Collectors.toList()))
                .thenAccept(topics -> {
                    listeners.forEach(listener -> {
                        topics.forEach(topic -> listener.load(TopicName.get(topic)));
                    });
                }).exceptionally(ex -> {
                    log.error("unLoad bundle :{} error", bundle);
                    return null;
                });
    }

    @Override
    public boolean test(NamespaceBundle namespaceBundle) {
        return true;
    }
}
