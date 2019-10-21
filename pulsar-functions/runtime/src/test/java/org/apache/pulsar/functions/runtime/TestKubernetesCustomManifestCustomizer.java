package org.apache.pulsar.functions.runtime;

import io.kubernetes.client.models.V1StatefulSet;
import org.apache.pulsar.functions.proto.Function;

import static org.testng.Assert.assertEquals;

class TestKubernetesCustomManifestCustomizer implements KubernetesManifestCustomizer {
    @Override
    public void init(Function.FunctionDetails funcDetails, String staticConfig) {
        assertEquals(funcDetails.getCustomRuntimeOptions(), "custom-service-account");
    }

    @Override
    public V1StatefulSet customizeStatefulSet(V1StatefulSet statefulSet) {
        statefulSet.getSpec().getTemplate().getSpec().serviceAccountName("my-service-account");
        return statefulSet;
    }
}
