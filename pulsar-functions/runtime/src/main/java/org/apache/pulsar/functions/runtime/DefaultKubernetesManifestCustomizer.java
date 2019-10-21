package org.apache.pulsar.functions.runtime;

import io.kubernetes.client.models.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.proto.Function;

import java.util.List;
import java.util.Map;

public class DefaultKubernetesManifestCustomizer implements KubernetesManifestCustomizer {

    @Getter
    @Setter
    @NoArgsConstructor
    static private class RuntimeOpts {
        private String jobNamespace;
        private Map<String, String> extraLabels;
        private Map<String, String> extraAnnotations;
        private Map<String, String> nodeSelectorLabels;
        private List<V1Toleration> tolerations;
    }

    private RuntimeOpts opts;

    @Override
    public void init(Function.FunctionDetails funcDetails, String staticConfig) {
        String customRuntimeOptions = funcDetails.getCustomRuntimeOptions();
        this.opts = new Gson().fromJson(customRuntimeOptions, RuntimeOpts.class);
        // ensure that we always have at least the default
        if (this.opts == null) {
            this.opts = new RuntimeOpts();
        }
    }

    @Override
    public String customizeNamespace(String currentNamespace) {
        if (!StringUtils.isEmpty(this.opts.getJobNamespace())) {
            return this.opts.getJobNamespace();
        } else {
            return currentNamespace;
        }
    }

    @Override
    public V1Service customizeService(V1Service service) {
        service.setMetadata(updateMeta(service.getMetadata()));
        return service;
    }

    @Override
    public V1StatefulSet customizeStatefulSet(V1StatefulSet statefulSet) {
        statefulSet.setMetadata(updateMeta(statefulSet.getMetadata()));
        V1PodTemplateSpec pt = statefulSet.getSpec().getTemplate();
        pt.setMetadata(updateMeta(pt.getMetadata()));
        V1PodSpec ps = pt.getSpec();
        if (this.opts.getNodeSelectorLabels() != null && this.opts.getNodeSelectorLabels().size() > 0) {
            this.opts.getNodeSelectorLabels().forEach(ps::putNodeSelectorItem);
        }
        if (this.opts.getTolerations() != null && this.opts.getTolerations().size() > 0) {
            this.opts.getTolerations().forEach(ps::addTolerationsItem);
        }
        return statefulSet;
    }

    private V1ObjectMeta updateMeta(V1ObjectMeta meta) {
        if (this.opts.getExtraAnnotations() != null && this.opts.getExtraAnnotations().size() > 0) {
            this.opts.getExtraAnnotations().forEach(meta::putAnnotationsItem);
        }
        if (this.opts.getExtraLabels() != null && this.opts.getExtraLabels().size() > 0) {
            this.opts.getExtraLabels().forEach(meta::putLabelsItem);
        }
        return meta;
    }

}
