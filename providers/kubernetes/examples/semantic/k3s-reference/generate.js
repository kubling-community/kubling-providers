#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const outputDirectory = __dirname;
const fragmentPath = path.join(outputDirectory, "fragment.yaml");
const provenancePath = path.join(outputDirectory, "provenance.yaml");
const profileName = "kubernetes-k3s-reference";
const profileVersion = "1.0.0";
const sourceVersion = "v1.36.1-k3s1";
const sourceImage = `docker.io/rancher/k3s:${sourceVersion}`;
const sourceImageDigest =
  "sha256:08fdebd14db9ab7d5ea821d5bfa95d02341a6ef886842fcc8d9dfd0e9fa9e0cd";
const fieldExpansionDepth = 6;

const namespaced = new Set([
  "Addon",
  "ConfigMap",
  "ControllerRevision",
  "CoreEvent",
  "CronJob",
  "CSIStorageCapacity",
  "DaemonSet",
  "Deployment",
  "Endpoints",
  "EndpointSlice",
  "Event",
  "HelmChart",
  "HelmChartConfig",
  "HorizontalPodAutoscaler",
  "Ingress",
  "Job",
  "Lease",
  "LimitRange",
  "NetworkPolicy",
  "PersistentVolumeClaim",
  "Pod",
  "PodDisruptionBudget",
  "PodTemplate",
  "ReplicationController",
  "ReplicaSet",
  "ResourceClaim",
  "ResourceClaimTemplate",
  "ResourceQuota",
  "Role",
  "RoleBinding",
  "Secret",
  "Service",
  "ServiceAccount",
  "StatefulSet"
]);

const entities = [
  ["Addon", "ADDON"],
  ["APIService", "API_SERVICE"],
  ["CertificateSigningRequest", "CERTIFICATE_SIGNING_REQUEST"],
  ["ClusterRole", "CLUSTER_ROLE"],
  ["ClusterRoleBinding", "CLUSTER_ROLE_BINDING"],
  ["ComponentStatus", "COMPONENT_STATUS"],
  ["ConfigMap", "CONFIG_MAP"],
  ["ControllerRevision", "CONTROLLER_REVISION"],
  ["CoreEvent", "CORE_EVENT"],
  ["CronJob", "CRON_JOB"],
  ["CSIDriver", "CSI_DRIVER"],
  ["CSINode", "CSI_NODE"],
  ["CSIStorageCapacity", "CSI_STORAGE_CAPACITY"],
  ["CustomResourceDefinition", "CUSTOM_RESOURCE_DEFINITION"],
  ["DaemonSet", "DAEMON_SET"],
  ["Deployment", "DEPLOYMENT"],
  ["DeviceClass", "DEVICE_CLASS"],
  ["Endpoints", "ENDPOINTS"],
  ["EndpointSlice", "ENDPOINT_SLICE"],
  ["ETCDSnapshotFile", "ETCD_SNAPSHOT_FILE"],
  ["Event", "EVENTS_K8S_IO_EVENT"],
  ["FlowSchema", "FLOW_SCHEMA"],
  ["HelmChart", "HELM_CHART"],
  ["HelmChartConfig", "HELM_CHART_CONFIG"],
  ["HorizontalPodAutoscaler", "HORIZONTAL_POD_AUTOSCALER"],
  ["Ingress", "INGRESS"],
  ["IngressClass", "INGRESS_CLASS"],
  ["IPAddress", "IP_ADDRESS"],
  ["Job", "JOB"],
  ["Lease", "LEASE"],
  ["LimitRange", "LIMIT_RANGE"],
  ["MutatingWebhookConfiguration", "MUTATING_WEBHOOK_CONFIGURATION"],
  ["Namespace", "NAMESPACE"],
  ["NetworkPolicy", "NETWORK_POLICY"],
  ["Node", "NODE"],
  ["PersistentVolume", "PERSISTENT_VOLUME"],
  ["PersistentVolumeClaim", "PERSISTENT_VOLUME_CLAIM"],
  ["Pod", "POD"],
  ["PodDisruptionBudget", "POD_DISRUPTION_BUDGET"],
  ["PodTemplate", "POD_TEMPLATE"],
  ["PriorityClass", "PRIORITY_CLASS"],
  ["PriorityLevelConfiguration", "PRIORITY_LEVEL_CONFIGURATION"],
  ["ReplicationController", "REPLICATION_CONTROLLER"],
  ["ReplicaSet", "REPLICA_SET"],
  ["ResourceClaim", "RESOURCE_CLAIM"],
  ["ResourceClaimTemplate", "RESOURCE_CLAIM_TEMPLATE"],
  ["ResourceQuota", "RESOURCE_QUOTA"],
  ["ResourceSlice", "RESOURCE_SLICE"],
  ["Role", "ROLE"],
  ["RoleBinding", "ROLE_BINDING"],
  ["RuntimeClass", "RUNTIME_CLASS"],
  ["Secret", "SECRET"],
  ["Service", "SERVICE"],
  ["ServiceAccount", "SERVICE_ACCOUNT"],
  ["ServiceCIDR", "SERVICE_CIDR"],
  ["StatefulSet", "STATEFUL_SET"],
  ["StorageClass", "STORAGE_CLASS"],
  ["ValidatingAdmissionPolicy", "VALIDATING_ADMISSION_POLICY"],
  ["ValidatingAdmissionPolicyBinding", "VALIDATING_ADMISSION_POLICY_BINDING"],
  ["ValidatingWebhookConfiguration", "VALIDATING_WEBHOOK_CONFIGURATION"],
  ["VolumeAttachment", "VOLUME_ATTACHMENT"],
  ["VolumeAttributesClass", "VOLUME_ATTRIBUTES_CLASS"]
].map(([id, table]) => ({id, table, namespaced: namespaced.has(id)}));

const property = (id, field, type = "string", name = id) => ({id, field, type, name});

const extraProperties = new Map([
  ["APIService", [
    property("serviceNamespace", "spec__service__namespace"),
    property("serviceName", "spec__service__name"),
    property("servicePort", "spec__service__port", "integer")
  ]],
  ["ClusterRoleBinding", [
    property("roleApiGroup", "roleRef__apiGroup"),
    property("roleKind", "roleRef__kind"),
    property("roleName", "roleRef__name")
  ]],
  ["CoreEvent", [
    property("involvedObjectApiVersion", "involvedObject__apiVersion"),
    property("involvedObjectKind", "involvedObject__kind"),
    property("involvedObjectName", "involvedObject__name"),
    property("involvedObjectNamespace", "involvedObject__namespace"),
    property("involvedObjectUid", "involvedObject__uid"),
    property("relatedApiVersion", "related__apiVersion"),
    property("relatedKind", "related__kind"),
    property("relatedName", "related__name"),
    property("relatedNamespace", "related__namespace"),
    property("relatedUid", "related__uid")
  ]],
  ["CSIStorageCapacity", [property("storageClassName", "storageClassName")]],
  ["CustomResourceDefinition", [
    property("conversionServiceNamespace", "spec__conversion__webhook__clientConfig__service__namespace"),
    property("conversionServiceName", "spec__conversion__webhook__clientConfig__service__name")
  ]],
  ["Deployment", [
    property("templateNodeName", "spec__template__spec__nodeName"),
    property("templateServiceAccountName", "spec__template__spec__serviceAccountName"),
    property("templatePriorityClassName", "spec__template__spec__priorityClassName"),
    property("templateRuntimeClassName", "spec__template__spec__runtimeClassName")
  ]],
  ["ETCDSnapshotFile", [property("nodeName", "spec__nodeName")]],
  ["Event", [
    property("regardingApiVersion", "regarding__apiVersion"),
    property("regardingKind", "regarding__kind"),
    property("regardingName", "regarding__name"),
    property("regardingNamespace", "regarding__namespace"),
    property("regardingUid", "regarding__uid"),
    property("relatedApiVersion", "related__apiVersion"),
    property("relatedKind", "related__kind"),
    property("relatedName", "related__name"),
    property("relatedNamespace", "related__namespace"),
    property("relatedUid", "related__uid")
  ]],
  ["FlowSchema", [property("priorityLevelName", "spec__priorityLevelConfiguration__name")]],
  ["HelmChart", [
    property("targetNamespace", "spec__targetNamespace"),
    property("jobName", "status__jobName"),
    property("authSecretName", "spec__authSecret__name"),
    property("dockerRegistrySecretName", "spec__dockerRegistrySecret__name"),
    property("repoCAConfigMapName", "spec__repoCAConfigMap__name")
  ]],
  ["HorizontalPodAutoscaler", [
    property("targetApiVersion", "spec__scaleTargetRef__apiVersion"),
    property("targetKind", "spec__scaleTargetRef__kind"),
    property("targetName", "spec__scaleTargetRef__name")
  ]],
  ["Ingress", [
    property("ingressClassName", "spec__ingressClassName"),
    property("defaultServiceName", "spec__defaultBackend__service__name")
  ]],
  ["IngressClass", [
    property("parameterApiGroup", "spec__parameters__apiGroup"),
    property("parameterKind", "spec__parameters__kind"),
    property("parameterName", "spec__parameters__name"),
    property("parameterNamespace", "spec__parameters__namespace"),
    property("parameterScope", "spec__parameters__scope")
  ]],
  ["IPAddress", [
    property("parentGroup", "spec__parentRef__group"),
    property("parentResource", "spec__parentRef__resource"),
    property("parentNamespace", "spec__parentRef__namespace"),
    property("parentName", "spec__parentRef__name")
  ]],
  ["PersistentVolume", [
    property("storageClassName", "spec__storageClassName"),
    property("volumeAttributesClassName", "spec__volumeAttributesClassName"),
    property("claimApiVersion", "spec__claimRef__apiVersion"),
    property("claimKind", "spec__claimRef__kind"),
    property("claimNamespace", "spec__claimRef__namespace"),
    property("claimName", "spec__claimRef__name"),
    property("claimUid", "spec__claimRef__uid"),
    property("csiDriver", "spec__csi__driver")
  ]],
  ["PersistentVolumeClaim", [
    property("storageClassName", "spec__storageClassName"),
    property("volumeAttributesClassName", "spec__volumeAttributesClassName"),
    property("volumeName", "spec__volumeName"),
    property("currentVolumeAttributesClassName", "status__currentVolumeAttributesClassName")
  ]],
  ["Pod", [
    property("replicaSetUid", "replica_set__uid"),
    property("replicaSetName", "replica_set__name"),
    property("nodeName", "spec__nodeName"),
    property("nominatedNodeName", "status__nominatedNodeName"),
    property("serviceAccountName", "spec__serviceAccountName"),
    property("priorityClassName", "spec__priorityClassName"),
    property("runtimeClassName", "spec__runtimeClassName"),
    property("extendedResourceClaimName", "status__extendedResourceClaimStatus__resourceClaimName")
  ]],
  ["ReplicaSet", [
    property("deploymentUid", "deployment__uid"),
    property("deploymentName", "deployment__name"),
    property("templateNodeName", "spec__template__spec__nodeName"),
    property("templateServiceAccountName", "spec__template__spec__serviceAccountName"),
    property("templatePriorityClassName", "spec__template__spec__priorityClassName"),
    property("templateRuntimeClassName", "spec__template__spec__runtimeClassName")
  ]],
  ["ResourceSlice", [property("nodeName", "spec__nodeName")]],
  ["RoleBinding", [
    property("roleApiGroup", "roleRef__apiGroup"),
    property("roleKind", "roleRef__kind"),
    property("roleName", "roleRef__name")
  ]],
  ["Service", [property("clusterIP", "spec__clusterIP")]],
  ["StatefulSet", [
    property("serviceName", "spec__serviceName"),
    property("currentRevision", "status__currentRevision"),
    property("updateRevision", "status__updateRevision"),
    property("templateNodeName", "spec__template__spec__nodeName"),
    property("templateServiceAccountName", "spec__template__spec__serviceAccountName"),
    property("templatePriorityClassName", "spec__template__spec__priorityClassName"),
    property("templateRuntimeClassName", "spec__template__spec__runtimeClassName")
  ]],
  ["StorageClass", [property("provisioner", "provisioner")]],
  ["ValidatingAdmissionPolicy", [
    property("parameterApiVersion", "spec__paramKind__apiVersion"),
    property("parameterKind", "spec__paramKind__kind")
  ]],
  ["ValidatingAdmissionPolicyBinding", [
    property("policyName", "spec__policyName"),
    property("parameterNamespace", "spec__paramRef__namespace"),
    property("parameterName", "spec__paramRef__name")
  ]],
  ["VolumeAttachment", [
    property("attacher", "spec__attacher"),
    property("nodeName", "spec__nodeName"),
    property("persistentVolumeName", "spec__source__persistentVolumeName")
  ]],
  ["VolumeAttributesClass", [property("driverName", "driverName")]]
]);

const templateFields = new Map([
  ["DaemonSet", "spec__template__spec__"],
  ["Job", "spec__template__spec__"],
  ["ReplicationController", "spec__template__spec__"],
  ["PodTemplate", "template__spec__"],
  ["CronJob", "spec__jobTemplate__spec__template__spec__"]
]);

for (const [entity, prefix] of templateFields) {
  extraProperties.set(entity, [
    property("templateNodeName", `${prefix}nodeName`),
    property("templateServiceAccountName", `${prefix}serviceAccountName`),
    property("templatePriorityClassName", `${prefix}priorityClassName`),
    property("templateRuntimeClassName", `${prefix}runtimeClassName`)
  ]);
}

const baseProperties = [
  property("apiVersion", "api_version", "string", "API version"),
  property("kind", "kind", "string", "Kubernetes kind"),
  property("uid", "metadata__uid", "string", "Kubernetes UID"),
  property("namespace", "metadata__namespace", "string", "Kubernetes namespace"),
  property("name", "metadata__name", "string", "Resource name"),
  property("labels", "metadata__labels", "json", "Labels"),
  property("ownerReferences", "metadata__ownerReferences", "json", "Owner references"),
  property("object", "object", "json", "Canonical Kubernetes document")
];

const relationships = [];

function addRelationship(id, from, to, cardinality, description, joins = []) {
  relationships.push({id, from, to, cardinality, description, joins});
}

function join(leftEntity, leftProperty, rightEntity, rightProperty) {
  return {
    left: `${leftEntity}.${leftProperty}`,
    right: `${rightEntity}.${rightProperty}`
  };
}

function namespacedNameJoins(from, namespaceProperty, nameProperty, to) {
  return [
    join(from, namespaceProperty, to, "namespace"),
    join(from, nameProperty, to, "name")
  ];
}

function clusterNameJoins(from, nameProperty, to) {
  return [join(from, nameProperty, to, "name")];
}

for (const entity of entities.filter(candidate => candidate.namespaced)) {
  addRelationship(
    `${entity.id}BelongsToNamespace`,
    entity.id,
    "Namespace",
    "manyToOne",
    `${entity.id} is scoped by a Kubernetes Namespace.`,
    [join(entity.id, "namespace", "Namespace", "name")]
  );
}

addRelationship(
  "DeploymentOwnsReplicaSet",
  "Deployment",
  "ReplicaSet",
  "oneToMany",
  "A Deployment controls ReplicaSets through Kubernetes owner references exposed by the provider.",
  [join("Deployment", "uid", "ReplicaSet", "deploymentUid")]
);
addRelationship(
  "ReplicaSetOwnsPod",
  "ReplicaSet",
  "Pod",
  "oneToMany",
  "A ReplicaSet controls Pods through Kubernetes owner references exposed by the provider.",
  [join("ReplicaSet", "uid", "Pod", "replicaSetUid")]
);
addRelationship(
  "DeploymentOwnsPod",
  "Deployment",
  "Pod",
  "oneToMany",
  "Transitive ownership through DeploymentOwnsReplicaSet and ReplicaSetOwnsPod."
);

for (const [owner, child] of [
  ["CronJob", "Job"],
  ["Job", "Pod"],
  ["DaemonSet", "Pod"],
  ["StatefulSet", "Pod"],
  ["ReplicationController", "Pod"],
  ["DaemonSet", "ControllerRevision"],
  ["StatefulSet", "ControllerRevision"],
  ["ResourceClaimTemplate", "ResourceClaim"],
  ["StatefulSet", "PersistentVolumeClaim"]
]) {
  addRelationship(
    `${owner}Owns${child}`,
    owner,
    child,
    "oneToMany",
    `${owner} owns ${child} resources through owner references; the array reference requires normalization.`
  );
}

for (const controller of [
  "Deployment",
  "ReplicaSet",
  "DaemonSet",
  "StatefulSet",
  "ReplicationController",
  "Job"
]) {
  addRelationship(
    `${controller}SelectsPod`,
    controller,
    "Pod",
    "oneToMany",
    `${controller} selects Pods using Kubernetes label-selector semantics.`
  );
}

const templateOwners = [
  "Deployment",
  "ReplicaSet",
  "DaemonSet",
  "StatefulSet",
  "ReplicationController",
  "Job",
  "CronJob",
  "PodTemplate"
];

for (const owner of templateOwners) {
  addRelationship(
    `${owner}TemplatePinsToNode`,
    owner,
    "Node",
    "manyToOne",
    `${owner} has an explicitly pinned Pod template node.`,
    clusterNameJoins(owner, "templateNodeName", "Node")
  );
  addRelationship(
    `${owner}TemplateUsesServiceAccount`,
    owner,
    "ServiceAccount",
    "manyToOne",
    `${owner} Pod templates use a ServiceAccount in the same namespace.`,
    namespacedNameJoins(owner, "namespace", "templateServiceAccountName", "ServiceAccount")
  );
  addRelationship(
    `${owner}TemplateUsesPriorityClass`,
    owner,
    "PriorityClass",
    "manyToOne",
    `${owner} Pod templates request a PriorityClass.`,
    clusterNameJoins(owner, "templatePriorityClassName", "PriorityClass")
  );
  addRelationship(
    `${owner}TemplateUsesRuntimeClass`,
    owner,
    "RuntimeClass",
    "manyToOne",
    `${owner} Pod templates request a RuntimeClass.`,
    clusterNameJoins(owner, "templateRuntimeClassName", "RuntimeClass")
  );

  for (const target of [
    "ConfigMap",
    "Secret",
    "PersistentVolumeClaim",
    "ResourceClaim",
    "ResourceClaimTemplate"
  ]) {
    addRelationship(
      `${owner}TemplateUses${target}`,
      owner,
      target,
      "manyToMany",
      `${owner} Pod templates can reference ${target} resources through array-valued PodSpec fields.`
    );
  }
}

addRelationship(
  "PodScheduledToNode",
  "Pod",
  "Node",
  "manyToOne",
  "A scheduled Pod names its assigned Node.",
  clusterNameJoins("Pod", "nodeName", "Node")
);
addRelationship(
  "PodNominatedToNode",
  "Pod",
  "Node",
  "manyToOne",
  "A pending Pod may name a nominated Node.",
  clusterNameJoins("Pod", "nominatedNodeName", "Node")
);
addRelationship(
  "PodUsesServiceAccount",
  "Pod",
  "ServiceAccount",
  "manyToOne",
  "A Pod uses a ServiceAccount in its namespace.",
  namespacedNameJoins("Pod", "namespace", "serviceAccountName", "ServiceAccount")
);
addRelationship(
  "PodUsesPriorityClass",
  "Pod",
  "PriorityClass",
  "manyToOne",
  "A Pod requests a PriorityClass.",
  clusterNameJoins("Pod", "priorityClassName", "PriorityClass")
);
addRelationship(
  "PodUsesRuntimeClass",
  "Pod",
  "RuntimeClass",
  "manyToOne",
  "A Pod requests a RuntimeClass.",
  clusterNameJoins("Pod", "runtimeClassName", "RuntimeClass")
);
addRelationship(
  "PodReportsExtendedResourceClaim",
  "Pod",
  "ResourceClaim",
  "manyToOne",
  "A Pod status may expose the ResourceClaim created for an extended resource request.",
  namespacedNameJoins("Pod", "namespace", "extendedResourceClaimName", "ResourceClaim")
);
for (const target of [
  "ConfigMap",
  "Secret",
  "PersistentVolumeClaim",
  "ResourceClaim",
  "ResourceClaimTemplate"
]) {
  addRelationship(
    `PodUses${target}`,
    "Pod",
    target,
    "manyToMany",
    `Pods can reference ${target} resources through array-valued PodSpec fields.`
  );
}

addRelationship(
  "APIServiceUsesService",
  "APIService",
  "Service",
  "manyToOne",
  "An aggregated APIService can delegate to a namespaced Service.",
  namespacedNameJoins("APIService", "serviceNamespace", "serviceName", "Service")
);
addRelationship(
  "CustomResourceDefinitionUsesConversionService",
  "CustomResourceDefinition",
  "Service",
  "manyToOne",
  "A CustomResourceDefinition conversion webhook can call a Service.",
  namespacedNameJoins(
    "CustomResourceDefinition",
    "conversionServiceNamespace",
    "conversionServiceName",
    "Service"
  )
);
addRelationship(
  "EndpointsDescribeService",
  "Endpoints",
  "Service",
  "oneToOne",
  "Legacy Endpoints share namespace and name with their Service.",
  namespacedNameJoins("Endpoints", "namespace", "name", "Service")
);
addRelationship(
  "EndpointSliceDescribesService",
  "EndpointSlice",
  "Service",
  "manyToOne",
  "EndpointSlices reference Services through the kubernetes.io/service-name label."
);
addRelationship(
  "EndpointSliceTargetsPod",
  "EndpointSlice",
  "Pod",
  "manyToMany",
  "EndpointSlice endpoints may target Pods through array-valued target references."
);
addRelationship(
  "EndpointSliceTargetsNode",
  "EndpointSlice",
  "Node",
  "manyToMany",
  "EndpointSlice endpoints may identify Nodes through array-valued node names or target references."
);
addRelationship(
  "ServiceSelectsPod",
  "Service",
  "Pod",
  "manyToMany",
  "A Service selects Pods using Kubernetes label-selector semantics."
);
addRelationship(
  "ServiceAllocatesPrimaryIPAddress",
  "Service",
  "IPAddress",
  "manyToOne",
  "A Service primary cluster IP is represented by an IPAddress allocation object.",
  clusterNameJoins("Service", "clusterIP", "IPAddress")
);
addRelationship(
  "IPAddressAllocatedToService",
  "IPAddress",
  "Service",
  "manyToOne",
  "IPAddress parentRef can point to a Service; the plural resource discriminator requires normalization."
);
addRelationship(
  "ServiceCIDRContainsIPAddress",
  "ServiceCIDR",
  "IPAddress",
  "oneToMany",
  "A ServiceCIDR contains IPAddress allocations by CIDR membership."
);
addRelationship(
  "ServiceCIDRContainsService",
  "ServiceCIDR",
  "Service",
  "oneToMany",
  "A ServiceCIDR contains Service cluster IPs by CIDR membership."
);
addRelationship(
  "IngressUsesIngressClass",
  "Ingress",
  "IngressClass",
  "manyToOne",
  "An Ingress names its IngressClass.",
  clusterNameJoins("Ingress", "ingressClassName", "IngressClass")
);
addRelationship(
  "IngressUsesDefaultService",
  "Ingress",
  "Service",
  "manyToOne",
  "An Ingress default backend names a Service in the same namespace.",
  namespacedNameJoins("Ingress", "namespace", "defaultServiceName", "Service")
);
addRelationship(
  "IngressUsesService",
  "Ingress",
  "Service",
  "manyToMany",
  "Ingress rule backends reference Services through an array-valued rule document."
);
addRelationship(
  "IngressUsesTLSSecret",
  "Ingress",
  "Secret",
  "manyToMany",
  "Ingress TLS entries reference Secrets through an array-valued document."
);
addRelationship(
  "NetworkPolicySelectsPod",
  "NetworkPolicy",
  "Pod",
  "manyToMany",
  "A NetworkPolicy selects governed Pods using label-selector semantics."
);
addRelationship(
  "NetworkPolicySelectsPeerPod",
  "NetworkPolicy",
  "Pod",
  "manyToMany",
  "NetworkPolicy peers can select Pods through nested selectors."
);
addRelationship(
  "NetworkPolicySelectsPeerNamespace",
  "NetworkPolicy",
  "Namespace",
  "manyToMany",
  "NetworkPolicy peers can select Namespaces through nested selectors."
);
addRelationship(
  "PodDisruptionBudgetSelectsPod",
  "PodDisruptionBudget",
  "Pod",
  "manyToMany",
  "A PodDisruptionBudget selects Pods using label-selector semantics."
);
addRelationship(
  "MutatingWebhookConfigurationUsesService",
  "MutatingWebhookConfiguration",
  "Service",
  "manyToMany",
  "Mutating webhook client configurations can reference Services inside an array."
);
addRelationship(
  "ValidatingWebhookConfigurationUsesService",
  "ValidatingWebhookConfiguration",
  "Service",
  "manyToMany",
  "Validating webhook client configurations can reference Services inside an array."
);

addRelationship(
  "CSINodeDescribesNode",
  "CSINode",
  "Node",
  "oneToOne",
  "A CSINode has the same name as its Kubernetes Node.",
  clusterNameJoins("CSINode", "name", "Node")
);
addRelationship(
  "CSINodeOffersCSIDriver",
  "CSINode",
  "CSIDriver",
  "manyToMany",
  "CSINode driver entries reference CSIDrivers through an array."
);
addRelationship(
  "CSIStorageCapacityUsesStorageClass",
  "CSIStorageCapacity",
  "StorageClass",
  "manyToOne",
  "CSI storage capacity is reported for a StorageClass.",
  clusterNameJoins("CSIStorageCapacity", "storageClassName", "StorageClass")
);
addRelationship(
  "CSIStorageCapacitySelectsNode",
  "CSIStorageCapacity",
  "Node",
  "manyToMany",
  "CSI storage capacity applies to Nodes selected by topology."
);
addRelationship(
  "ETCDSnapshotFileStoredOnNode",
  "ETCDSnapshotFile",
  "Node",
  "manyToOne",
  "A local ETCD snapshot records its Node.",
  clusterNameJoins("ETCDSnapshotFile", "nodeName", "Node")
);
addRelationship(
  "PersistentVolumeBoundToClaim",
  "PersistentVolume",
  "PersistentVolumeClaim",
  "manyToOne",
  "A PersistentVolume claimRef names its bound PersistentVolumeClaim.",
  [
    join("PersistentVolume", "claimNamespace", "PersistentVolumeClaim", "namespace"),
    join("PersistentVolume", "claimName", "PersistentVolumeClaim", "name"),
    join("PersistentVolume", "claimUid", "PersistentVolumeClaim", "uid")
  ]
);
addRelationship(
  "PersistentVolumeUsesStorageClass",
  "PersistentVolume",
  "StorageClass",
  "manyToOne",
  "A PersistentVolume uses a StorageClass.",
  clusterNameJoins("PersistentVolume", "storageClassName", "StorageClass")
);
addRelationship(
  "PersistentVolumeUsesVolumeAttributesClass",
  "PersistentVolume",
  "VolumeAttributesClass",
  "manyToOne",
  "A PersistentVolume can use a VolumeAttributesClass.",
  clusterNameJoins("PersistentVolume", "volumeAttributesClassName", "VolumeAttributesClass")
);
addRelationship(
  "PersistentVolumeUsesCSIDriver",
  "PersistentVolume",
  "CSIDriver",
  "manyToOne",
  "A CSI PersistentVolume names its CSIDriver.",
  clusterNameJoins("PersistentVolume", "csiDriver", "CSIDriver")
);
addRelationship(
  "PersistentVolumeSelectsNode",
  "PersistentVolume",
  "Node",
  "manyToMany",
  "PersistentVolume node affinity selects Nodes."
);
addRelationship(
  "PersistentVolumeClaimBoundToVolume",
  "PersistentVolumeClaim",
  "PersistentVolume",
  "manyToOne",
  "A bound PersistentVolumeClaim names its PersistentVolume.",
  clusterNameJoins("PersistentVolumeClaim", "volumeName", "PersistentVolume")
);
addRelationship(
  "PersistentVolumeClaimUsesStorageClass",
  "PersistentVolumeClaim",
  "StorageClass",
  "manyToOne",
  "A PersistentVolumeClaim requests a StorageClass.",
  clusterNameJoins("PersistentVolumeClaim", "storageClassName", "StorageClass")
);
addRelationship(
  "PersistentVolumeClaimUsesVolumeAttributesClass",
  "PersistentVolumeClaim",
  "VolumeAttributesClass",
  "manyToOne",
  "A PersistentVolumeClaim requests a VolumeAttributesClass.",
  clusterNameJoins("PersistentVolumeClaim", "volumeAttributesClassName", "VolumeAttributesClass")
);
addRelationship(
  "PersistentVolumeClaimUsesCurrentVolumeAttributesClass",
  "PersistentVolumeClaim",
  "VolumeAttributesClass",
  "manyToOne",
  "A PersistentVolumeClaim reports its current VolumeAttributesClass.",
  clusterNameJoins(
    "PersistentVolumeClaim",
    "currentVolumeAttributesClassName",
    "VolumeAttributesClass"
  )
);
addRelationship(
  "ResourceSlicePublishedForNode",
  "ResourceSlice",
  "Node",
  "manyToOne",
  "A node-local ResourceSlice names its Node.",
  clusterNameJoins("ResourceSlice", "nodeName", "Node")
);
addRelationship(
  "ResourceSliceSelectsNode",
  "ResourceSlice",
  "Node",
  "manyToMany",
  "A ResourceSlice can select Nodes with a node selector."
);
addRelationship(
  "ResourceClaimRequestsDeviceClass",
  "ResourceClaim",
  "DeviceClass",
  "manyToMany",
  "ResourceClaim device requests reference DeviceClasses inside arrays."
);
addRelationship(
  "ResourceClaimAllocatedFromResourceSlice",
  "ResourceClaim",
  "ResourceSlice",
  "manyToMany",
  "ResourceClaim allocation results identify devices published by ResourceSlices."
);
addRelationship(
  "ResourceClaimReservedForPod",
  "ResourceClaim",
  "Pod",
  "manyToMany",
  "ResourceClaim reservedFor entries can reference Pods."
);
addRelationship(
  "ResourceClaimTemplateRequestsDeviceClass",
  "ResourceClaimTemplate",
  "DeviceClass",
  "manyToMany",
  "ResourceClaimTemplate device requests reference DeviceClasses inside arrays."
);
addRelationship(
  "RuntimeClassSelectsNode",
  "RuntimeClass",
  "Node",
  "manyToMany",
  "RuntimeClass scheduling constraints select Nodes."
);
addRelationship(
  "ServiceAccountUsesSecret",
  "ServiceAccount",
  "Secret",
  "manyToMany",
  "ServiceAccount secret and imagePullSecret arrays reference Secrets."
);
addRelationship(
  "StatefulSetUsesService",
  "StatefulSet",
  "Service",
  "manyToOne",
  "A StatefulSet names the Service governing its network identity.",
  namespacedNameJoins("StatefulSet", "namespace", "serviceName", "Service")
);
addRelationship(
  "StatefulSetUsesCurrentControllerRevision",
  "StatefulSet",
  "ControllerRevision",
  "manyToOne",
  "A StatefulSet reports its current ControllerRevision.",
  namespacedNameJoins("StatefulSet", "namespace", "currentRevision", "ControllerRevision")
);
addRelationship(
  "StatefulSetUsesUpdateControllerRevision",
  "StatefulSet",
  "ControllerRevision",
  "manyToOne",
  "A StatefulSet reports its update ControllerRevision.",
  namespacedNameJoins("StatefulSet", "namespace", "updateRevision", "ControllerRevision")
);
addRelationship(
  "StorageClassUsesCSIDriver",
  "StorageClass",
  "CSIDriver",
  "manyToOne",
  "A CSI StorageClass provisioner can match a CSIDriver.",
  clusterNameJoins("StorageClass", "provisioner", "CSIDriver")
);
addRelationship(
  "VolumeAttachmentTargetsNode",
  "VolumeAttachment",
  "Node",
  "manyToOne",
  "A VolumeAttachment names its target Node.",
  clusterNameJoins("VolumeAttachment", "nodeName", "Node")
);
addRelationship(
  "VolumeAttachmentUsesCSIDriver",
  "VolumeAttachment",
  "CSIDriver",
  "manyToOne",
  "A VolumeAttachment names the CSI attacher responsible for it.",
  clusterNameJoins("VolumeAttachment", "attacher", "CSIDriver")
);
addRelationship(
  "VolumeAttachmentAttachesPersistentVolume",
  "VolumeAttachment",
  "PersistentVolume",
  "manyToOne",
  "A VolumeAttachment source names its PersistentVolume.",
  clusterNameJoins("VolumeAttachment", "persistentVolumeName", "PersistentVolume")
);
addRelationship(
  "VolumeAttributesClassUsesCSIDriver",
  "VolumeAttributesClass",
  "CSIDriver",
  "manyToOne",
  "A VolumeAttributesClass names its CSIDriver.",
  clusterNameJoins("VolumeAttributesClass", "driverName", "CSIDriver")
);

addRelationship(
  "ClusterRoleBindingUsesClusterRole",
  "ClusterRoleBinding",
  "ClusterRole",
  "manyToOne",
  "A ClusterRoleBinding roleRef references a ClusterRole.",
  [
    join("ClusterRoleBinding", "roleKind", "ClusterRole", "kind"),
    join("ClusterRoleBinding", "roleName", "ClusterRole", "name")
  ]
);
addRelationship(
  "RoleBindingUsesRole",
  "RoleBinding",
  "Role",
  "manyToOne",
  "A RoleBinding can reference a Role in the same namespace.",
  [
    join("RoleBinding", "namespace", "Role", "namespace"),
    join("RoleBinding", "roleKind", "Role", "kind"),
    join("RoleBinding", "roleName", "Role", "name")
  ]
);
addRelationship(
  "RoleBindingUsesClusterRole",
  "RoleBinding",
  "ClusterRole",
  "manyToOne",
  "A RoleBinding can reference a ClusterRole.",
  [
    join("RoleBinding", "roleKind", "ClusterRole", "kind"),
    join("RoleBinding", "roleName", "ClusterRole", "name")
  ]
);
addRelationship(
  "ClusterRoleBindingIncludesServiceAccount",
  "ClusterRoleBinding",
  "ServiceAccount",
  "manyToMany",
  "ClusterRoleBinding subject arrays can reference ServiceAccounts."
);
addRelationship(
  "RoleBindingIncludesServiceAccount",
  "RoleBinding",
  "ServiceAccount",
  "manyToMany",
  "RoleBinding subject arrays can reference ServiceAccounts."
);
addRelationship(
  "ClusterRoleAggregatesClusterRole",
  "ClusterRole",
  "ClusterRole",
  "manyToMany",
  "A ClusterRole aggregation rule selects other ClusterRoles by label."
);
addRelationship(
  "ValidatingAdmissionPolicyBindingUsesPolicy",
  "ValidatingAdmissionPolicyBinding",
  "ValidatingAdmissionPolicy",
  "manyToOne",
  "A ValidatingAdmissionPolicyBinding names its policy.",
  clusterNameJoins(
    "ValidatingAdmissionPolicyBinding",
    "policyName",
    "ValidatingAdmissionPolicy"
  )
);
addRelationship(
  "ValidatingAdmissionPolicyParameterKindDefinedByCustomResourceDefinition",
  "ValidatingAdmissionPolicy",
  "CustomResourceDefinition",
  "manyToOne",
  "A policy parameter kind can be defined by a CustomResourceDefinition; type normalization is required."
);

addRelationship(
  "FlowSchemaUsesPriorityLevelConfiguration",
  "FlowSchema",
  "PriorityLevelConfiguration",
  "manyToOne",
  "A FlowSchema names its PriorityLevelConfiguration.",
  clusterNameJoins("FlowSchema", "priorityLevelName", "PriorityLevelConfiguration")
);
addRelationship(
  "HelmChartConfigConfiguresHelmChart",
  "HelmChartConfig",
  "HelmChart",
  "oneToOne",
  "A k3s HelmChartConfig matches a HelmChart by namespace and name.",
  namespacedNameJoins("HelmChartConfig", "namespace", "name", "HelmChart")
);
addRelationship(
  "HelmChartTargetsNamespace",
  "HelmChart",
  "Namespace",
  "manyToOne",
  "A HelmChart can install into a target Namespace.",
  [join("HelmChart", "targetNamespace", "Namespace", "name")]
);
addRelationship(
  "HelmChartRunsJob",
  "HelmChart",
  "Job",
  "manyToOne",
  "A HelmChart status identifies its installation Job.",
  namespacedNameJoins("HelmChart", "namespace", "jobName", "Job")
);
addRelationship(
  "HelmChartUsesAuthSecret",
  "HelmChart",
  "Secret",
  "manyToOne",
  "A HelmChart can use a repository authentication Secret.",
  namespacedNameJoins("HelmChart", "namespace", "authSecretName", "Secret")
);
addRelationship(
  "HelmChartUsesDockerRegistrySecret",
  "HelmChart",
  "Secret",
  "manyToOne",
  "A HelmChart can use an OCI registry Secret.",
  namespacedNameJoins("HelmChart", "namespace", "dockerRegistrySecretName", "Secret")
);
addRelationship(
  "HelmChartUsesRepoCAConfigMap",
  "HelmChart",
  "ConfigMap",
  "manyToOne",
  "A HelmChart can load repository CAs from a ConfigMap.",
  namespacedNameJoins("HelmChart", "namespace", "repoCAConfigMapName", "ConfigMap")
);
addRelationship(
  "HelmChartUsesValuesSecret",
  "HelmChart",
  "Secret",
  "manyToMany",
  "A HelmChart valuesSecrets array can reference Secrets."
);
addRelationship(
  "HelmChartConfigUsesValuesSecret",
  "HelmChartConfig",
  "Secret",
  "manyToMany",
  "A HelmChartConfig valuesSecrets array can reference Secrets."
);

for (const target of [
  "Deployment",
  "StatefulSet",
  "ReplicaSet",
  "ReplicationController"
]) {
  addRelationship(
    `HorizontalPodAutoscalerScales${target}`,
    "HorizontalPodAutoscaler",
    target,
    "manyToOne",
    `A HorizontalPodAutoscaler scaleTargetRef can reference ${target}.`,
    [
      join("HorizontalPodAutoscaler", "namespace", target, "namespace"),
      join("HorizontalPodAutoscaler", "targetApiVersion", target, "apiVersion"),
      join("HorizontalPodAutoscaler", "targetKind", target, "kind"),
      join("HorizontalPodAutoscaler", "targetName", target, "name")
    ]
  );
}

addRelationship(
  "LeaseRepresentsNodeHeartbeat",
  "Lease",
  "Node",
  "oneToOne",
  "Leases in kube-node-lease conventionally share a name with their Node; the namespace predicate requires normalization."
);
addRelationship(
  "ResourceQuotaScopesPriorityClass",
  "ResourceQuota",
  "PriorityClass",
  "manyToMany",
  "ResourceQuota scope selectors can reference PriorityClass names."
);

for (const eventSpec of [
  {
    entity: "CoreEvent",
    relationPrefix: "CoreEventRegarding",
    description: "involvedObject",
    apiVersion: "involvedObjectApiVersion",
    kind: "involvedObjectKind",
    uid: "involvedObjectUid"
  },
  {
    entity: "CoreEvent",
    relationPrefix: "CoreEventRelatedTo",
    description: "related",
    apiVersion: "relatedApiVersion",
    kind: "relatedKind",
    uid: "relatedUid"
  },
  {
    entity: "Event",
    relationPrefix: "EventRegarding",
    description: "regarding",
    apiVersion: "regardingApiVersion",
    kind: "regardingKind",
    uid: "regardingUid"
  },
  {
    entity: "Event",
    relationPrefix: "EventRelatedTo",
    description: "related",
    apiVersion: "relatedApiVersion",
    kind: "relatedKind",
    uid: "relatedUid"
  }
]) {
  for (const target of entities) {
    addRelationship(
      `${eventSpec.relationPrefix}${target.id}`,
      eventSpec.entity,
      target.id,
      "manyToOne",
      `${eventSpec.entity} ${eventSpec.description} can reference a ${target.id}.`,
      [
        join(eventSpec.entity, eventSpec.apiVersion, target.id, "apiVersion"),
        join(eventSpec.entity, eventSpec.kind, target.id, "kind"),
        join(eventSpec.entity, eventSpec.uid, target.id, "uid")
      ]
    );
  }
}

const expectedTables = 62;
const expectedRelationships = 457;
const expectedExecutableRelationships = 363;
if (entities.length !== expectedTables) {
  throw new Error(`Expected ${expectedTables} entities, found ${entities.length}.`);
}
if (new Set(entities.map(entity => entity.id)).size !== entities.length) {
  throw new Error("Duplicate semantic entity id.");
}
if (new Set(entities.map(entity => entity.table)).size !== entities.length) {
  throw new Error("Duplicate Kubernetes table binding.");
}
if (new Set(relationships.map(relationship => relationship.id)).size !== relationships.length) {
  throw new Error("Duplicate semantic relationship id.");
}
if (relationships.length !== expectedRelationships) {
  throw new Error(
    `Expected ${expectedRelationships} relationships, found ${relationships.length}.`
  );
}

const propertiesByEntity = new Map(entities.map(entity => {
  if (entity.table.includes(".")) {
    throw new Error(`Entity ${entity.id} has a qualified relation binding.`);
  }
  const properties = [...baseProperties, ...(extraProperties.get(entity.id) || [])];
  for (const candidate of properties) {
    if (candidate.field.includes(".")) {
      throw new Error(`Property ${entity.id}.${candidate.id} has a qualified field binding.`);
    }
  }
  return [entity.id, new Set(properties.map(candidate => candidate.id))];
}));

function validatePropertyReference(reference, expectedEntity, relationshipId) {
  const [entityId, propertyId, extra] = reference.split(".");
  if (extra !== undefined || entityId !== expectedEntity) {
    throw new Error(
      `Relationship ${relationshipId} has invalid reference ${reference}; ` +
      `expected ${expectedEntity}.<property>.`
    );
  }
  if (!propertiesByEntity.get(entityId)?.has(propertyId)) {
    throw new Error(`Relationship ${relationshipId} references unknown property ${reference}.`);
  }
}

for (const relationship of relationships) {
  if (!propertiesByEntity.has(relationship.from) || !propertiesByEntity.has(relationship.to)) {
    throw new Error(`Relationship ${relationship.id} has an unknown endpoint.`);
  }
  for (const condition of relationship.joins) {
    validatePropertyReference(condition.left, relationship.from, relationship.id);
    validatePropertyReference(condition.right, relationship.to, relationship.id);
  }
}

const executableRelationships = relationships.filter(
  relationship => relationship.joins.length > 0
);
const semanticOnlyRelationships = relationships.filter(
  relationship => relationship.joins.length === 0
);
if (executableRelationships.length !== expectedExecutableRelationships) {
  throw new Error(
    `Expected ${expectedExecutableRelationships} executable relationships, ` +
    `found ${executableRelationships.length}.`
  );
}

function quoted(value) {
  return JSON.stringify(value);
}

function renderProperty(item, indent) {
  return [
    `${indent}- id: ${quoted(item.id)}`,
    `${indent}  name: ${quoted(item.name)}`,
    `${indent}  type: ${quoted(item.type)}`,
    `${indent}  binding:`,
    `${indent}    field: ${quoted(item.field)}`
  ].join("\n");
}

function renderEntity(entity) {
  const properties = [...baseProperties, ...(extraProperties.get(entity.id) || [])];
  const naturalIdentity = entity.namespaced
    ? ["namespace", "name"]
    : ["name"];
  return [
    `    - id: ${quoted(entity.id)}`,
    `      name: ${quoted(entity.id)}`,
    `      description: ${quoted(`Kubernetes ${entity.id} resource exposed by ${entity.table}.`)}`,
    `      binding:`,
    `        relation: ${quoted(entity.table)}`,
    `      properties:`,
    ...properties.map(item => renderProperty(item, "        ")),
    `      identities:`,
    `        - id: ${quoted("uidIdentity")}`,
    `          properties:`,
    `            - ${quoted("uid")}`,
    `        - id: ${quoted(entity.namespaced ? "namespacedNameIdentity" : "nameIdentity")}`,
    `          properties:`,
    ...naturalIdentity.map(item => `            - ${quoted(item)}`)
  ].join("\n");
}

function renderRelationship(relationship) {
  const lines = [
    `    - id: ${quoted(relationship.id)}`,
    `      name: ${quoted(relationship.id)}`,
    `      description: ${quoted(relationship.description)}`,
    `      from: ${quoted(relationship.from)}`,
    `      to: ${quoted(relationship.to)}`,
    `      cardinality: ${quoted(relationship.cardinality)}`
  ];
  if (relationship.joins.length > 0) {
    lines.push("      joins:");
    for (const condition of relationship.joins) {
      lines.push(`        - left: ${quoted(condition.left)}`);
      lines.push(`          right: ${quoted(condition.right)}`);
    }
  }
  return lines.join("\n");
}

const fragment = [
  "# Generated by generate.js; do not edit by hand.",
  "format: kubling-semantic",
  "schemaVersion: 1",
  "kind: fragment",
  "",
  "metadata:",
  `  name: ${profileName}`,
  "  namespace: k8s",
  `  version: ${profileVersion}`,
  "",
  "spec:",
  "  entities:",
  entities.map(renderEntity).join("\n\n"),
  "",
  "  relationships:",
  "    # Executable relationships have exact source-local joins.",
  executableRelationships.map(renderRelationship).join("\n\n"),
  "",
  "    # Semantic-only relationships are useful domain links without an exact join.",
  semanticOnlyRelationships.map(renderRelationship).join("\n\n"),
  ""
].join("\n");

const fragmentDigest = `sha256:${crypto.createHash("sha256").update(fragment).digest("hex")}`;
const generatorDigest = `sha256:${crypto.createHash("sha256")
  .update(fs.readFileSync(__filename))
  .digest("hex")}`;
const provenance = [
  "# Generated by generate.js; do not edit by hand.",
  "profile:",
  `  name: ${profileName}`,
  `  version: ${profileVersion}`,
  "source:",
  "  distribution: k3s",
  `  version: ${sourceVersion}`,
  `  image: ${sourceImage}`,
  `  imageDigest: ${sourceImageDigest}`,
  "generator:",
  "  path: generate.js",
  `  digest: ${generatorDigest}`,
  "provider:",
  `  fieldExpansionDepth: ${fieldExpansionDepth}`,
  "  includeObject: true",
  "artifact:",
  "  path: fragment.yaml",
  `  digest: ${fragmentDigest}`,
  `  entities: ${entities.length}`,
  "  relationships:",
  `    executable: ${executableRelationships.length}`,
  `    semanticOnly: ${semanticOnlyRelationships.length}`,
  ""
].join("\n");

const generated = [
  [fragmentPath, fragment],
  [provenancePath, provenance]
];

if (process.argv.includes("--check")) {
  for (const [target, expected] of generated) {
    const actual = fs.existsSync(target) ? fs.readFileSync(target, "utf8") : null;
    if (actual !== expected) {
      console.error(`${path.relative(outputDirectory, target)} is not up to date.`);
      process.exitCode = 1;
    }
  }
} else {
  for (const [target, content] of generated) {
    fs.writeFileSync(target, content, "utf8");
  }
  console.log(
    `Generated ${entities.length} entities and ${relationships.length} relationships ` +
    `(${executableRelationships.length} executable, ` +
    `${semanticOnlyRelationships.length} semantic-only).`
  );
}
