package driver

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"crypto/sha1"

	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/cloud_provider/clientset"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Mounter pod spec defaults
	mounterPodNamePrefix    = "gcsfusecsi-mount"
	mounterPodCPURequest    = "250m"
	mounterPodMemoryRequest = "512Mi"
	mounterPodMemoryLimit   = "10Gi"
	mounterPodCPULimit      = "0"
	mounterPodMountDir      = "mount-dir"
	mounterPodEmptyDir      = "gke-gcsfusecsi-tmp"

	// Supported mountLocality values
	mountLocalityNode = "node"
	mountLocalityPod  = "pod"

	KubeletDir = "/var/lib/kubelet"
)

// NewNodeMounterManager creates a new nodeMounterManager.
func NewNodeMounterManager(k8sClient clientset.Interface, mounterPodNamespace, mounterPodImage, mounterPodPriorityClass string) *nodeMounterManager {
	return &nodeMounterManager{
		k8sClient:               k8sClient,
		mounterPodNamespace:     mounterPodNamespace,
		mounterPodImage:         mounterPodImage,
		mounterPodPriorityClass: mounterPodPriorityClass,
	}
}

type nodeMounterManager struct {
	k8sClient               clientset.Interface
	mounterPodNamespace     string
	mounterPodImage         string
	mounterPodPriorityClass string
}

type mounterPodConfig struct {
	podName             string
	nodeID              string
	memoryRequest       string
	cpuRequest          string
	memoryLimit         string
	cpuLimit            string
	image               string
	namespace           string
	priorityClass       string
	mountOptionsFlagMap map[string]string
	accessPoints        string
}

// mutateVolumeContextWithNodeLocalParams validates and updates the given volume context with known parameters related to node-local mounting.
func mutateVolumeContextWithNodeLocalParams(vc map[string]string, params map[string]string) error {
	vc[keyMountLocality] = mountLocalityPod // Default
	// Validate mountLocality flag and set it.
	nodeLocalMountSpecified := false
	if v, exists := params[keyMountLocality]; exists {
		if v != mountLocalityNode && v != mountLocalityPod {
			return fmt.Errorf("invalid mountLocality value: %v, expected %q or %q", v, mountLocalityPod, mountLocalityNode)
		}
		if v == mountLocalityNode {
			vc[keyMountLocality] = mountLocalityNode
			nodeLocalMountSpecified = true
		}
	}
	// Set dfuse known resource attributes. Validation will be done when parsed.
	validDfuseResourceAttrs := []string{}
	for _, attr := range validDfuseResourceAttrs {
		if v, exists := params[attr]; exists {
			if !nodeLocalMountSpecified {
				return fmt.Errorf("dfuse resource parameter %q is only compatible with node-local mount mode, which is not enabled", attr)
			}
			vc[attr] = v
		}
	}
	return nil
}

// isNodeLocalMount checks if the volume is configured for node-local mounting
// by examining the "mountLocality" key in the VolumeContext.
func isNodeLocalMount(vc map[string]string) bool {
	if v, exists := vc[keyMountLocality]; exists && v == mountLocalityNode {
		return true
	}
	return false
}

// createMounterPodName returns a unique name for the mounter pod. The name is suffixed by the node ID, project, location,
// and Parallelstore instance name evaluated on a SHA1 hash for length shortening.
func createMounterPodName(nodeID, project, location, instanceName string) string {
	str := fmt.Sprintf("%s_%s_%s_%s", nodeID, project, location, instanceName)
	h := sha1.New()
	// Write the string to the hash
	io.WriteString(h, str)
	// Convert the byte slice to a hexadecimal string
	sha1Hash := fmt.Sprintf("%x", h.Sum(nil))
	return fmt.Sprintf("%s-%s", mounterPodNamePrefix, sha1Hash)
}

// verifyMounterPodExists returns the mounter pod if it exists, or returns an error.
func (nm *nodeMounterManager) getMounterPod(podName string) (*corev1.Pod, error) {
	pod, err := nm.k8sClient.GetPod(nm.mounterPodNamespace, podName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return pod, nil
}

// createMounterPod handles the creation of the mounter pod using the Kubernetes API client.
func (nm *nodeMounterManager) createMounterPod(ctx context.Context, config *mounterPodConfig) error {
	// Check if the mounter pod already exists, but was marked for deletion.
	// This requires calling the API server directly to retrieve the most up-to-date pod status.
	pod, err := nm.k8sClient.GetPod(nm.mounterPodNamespace, config.podName)
	if err != nil && !errors.IsNotFound(err) {
		klog.Infof("Failed to get mounter pod %s: %v", config.podName, err)

		return err
	}

	// GET always returns a pointer to the pod, even if the pod doesn't exist.
	// Therefore, we cannot rely on a nil pointer to determine the pod's existence.
	if errors.IsNotFound(err) {
		podSpec, err := createMounterPodSpec(config)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		if _, err = nm.k8sClient.K8sClient().CoreV1().Pods(nm.mounterPodNamespace).Create(ctx, podSpec, metav1.CreateOptions{}); err != nil {
			return err
		}

		return nil
	}

	// If the mounter pod is marked for deletion, prevent ControllerPublishVolume from succeeding.
	if pod.ObjectMeta.DeletionTimestamp != nil {
		return status.Errorf(
			codes.Aborted,
			"Mounter pod %s/%s is marked for deletion. Waiting for pod deletion to complete.",
			nm.mounterPodNamespace, config.podName,
		)
	}
	klog.Infof("Mounter pod %s/%s already exists.", nm.mounterPodNamespace, config.podName)

	return nil
}

// TODO: ssyssy deleteMounterPod handles the deletion of the mounter pod, if it exists.
func (nm *nodeMounterManager) deleteMounterPod(ctx context.Context, podName string) error {
	return nil
}

// TODO(urielguzman): Re-visit this function when we have implemented mounter pod logic.
// createMounterPodSpec returns the pod spec for the mounter pod, or returns an error.
// It also sets mounter pod container resource requests/limits set by the user, as well
// as passing the necessary flags that the mounter pod needs to perform the dfuse mount
// and the creation of the daos_agent.
func createMounterPodSpec(config *mounterPodConfig) (*v1.Pod, error) {
	spec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.podName,
			Namespace: config.namespace,
			Labels:    map[string]string{
				// TODO(shensiyan): Add labels to the mounter pod.
			},
		},
		Spec: v1.PodSpec{
			NodeSelector: map[string]string{
				// Use NodeSelector rather than NodeName in the mounter pod spec,
				// since NodeName will bypass kube-scheduler.
				"kubernetes.io/hostname": config.nodeID,
				// Parallelstore is not supported on ARM nodes.
				// Example: https://source.corp.google.com/piper///depot/google3/cloud/kubernetes/distro/components/parallelstorecsi/0.4/parallelstorecsi_node.yaml;l=36-37
				"kubernetes.io/os":   "linux",
				"kubernetes.io/arch": "amd64",
			},
			PriorityClassName: config.priorityClass,
			Containers: []v1.Container{
				{
					Name:            mounterPodNamePrefix,
					Image:           config.image,
					ImagePullPolicy: v1.PullAlways,
					Args:            createMounterPodArgs(config),
					SecurityContext: &v1.SecurityContext{
						Privileged: proto.Bool(true),
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:             mounterPodMountDir,
							MountPath:        util.KubeletDir,
							MountPropagation: &[]v1.MountPropagationMode{v1.MountPropagationBidirectional}[0],
						},
						{
							Name:      mounterPodEmptyDir,
							MountPath: util.MounterPodEmptyDirMountPath,
						},
					},
					Env: []v1.EnvVar{
						{
							Name:  "D_LOG_MASK",
							Value: "INFO",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: mounterPodMountDir,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: util.KubeletDir,
							Type: &[]v1.HostPathType{v1.HostPathDirectoryOrCreate}[0],
						},
					},
				},
				{
					Name: mounterPodEmptyDir,
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				},
			},
			Tolerations: []v1.Toleration{
				{
					//  https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/
					//  See "special case". This will tolerate everything. Mounter pod should
					//  be possiblel to be scheduled on all nodes.
					Operator: v1.TolerationOpExists,
				},
			},
		},
	}

	resources, err := createMounterPodResources(config)
	if err != nil {
		return nil, err
	}
	spec.Spec.Containers[0].Resources = *resources

	return spec, nil
}

// createMounterPodResources returns the resource requirements for the mounter pod. It sets the
// default values first and overrides and resource requirements set by the user.
func createMounterPodResources(config *mounterPodConfig) (*v1.ResourceRequirements, error) {
	// Default values.
	resources := createMounterPodDefaultResourceReqs()

	// User overrides.
	if err := setResource(config.cpuRequest, v1.ResourceCPU, &resources.Requests); err != nil {
		return nil, err
	}
	if err := setResource(config.memoryRequest, v1.ResourceMemory, &resources.Requests); err != nil {
		return nil, err
	}
	if err := setResource(config.cpuLimit, v1.ResourceCPU, &resources.Limits); err != nil {
		return nil, err
	}
	if err := setResource(config.memoryLimit, v1.ResourceMemory, &resources.Limits); err != nil {
		return nil, err
	}

	return resources, nil
}

func setResource(value string, resourceType v1.ResourceName, resourceList *v1.ResourceList) error {
	if value == "" {
		return nil // No value provided, skip setting
	}

	val, err := resource.ParseQuantity(value)
	if err != nil {
		return fmt.Errorf("error parsing resource quantity %q: %w", value, err)
	}

	// Check for negative values
	if val.CmpInt64(0) < 0 {
		return fmt.Errorf("resource quantity cannot be negative: %s", value)
	}

	// Remove the value if "0" is specified.
	if val.IsZero() {
		delete(*resourceList, resourceType)
	} else {
		(*resourceList)[resourceType] = val
	}

	return nil
}

// createMounterPodDefaultRequestReqs returns the default resource requirements
// that the mounter pod will use if no custom resource requiremenents are
// specified.
func createMounterPodDefaultResourceReqs() *v1.ResourceRequirements {
	return &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse(mounterPodCPURequest),

			v1.ResourceMemory: resource.MustParse(mounterPodMemoryRequest),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse(mounterPodMemoryLimit),
		},
	}
}

// createMounterPodArgs sets the flags that are passed down to the mounter pod image.
func createMounterPodArgs(config *mounterPodConfig) []string {
	// allowedFlags represents the mount options that are allowed to be passed to the
	// mounter pod image as args (e.g. --eq-count)
	allowedFlags := map[string]bool{}

	var args []string

	// Mount options as flag args.
	for k, v := range config.mountOptionsFlagMap {
		if _, allowed := allowedFlags[k]; !allowed {
			continue
		}
		arg := fmt.Sprintf("--%s", k)
		if v != "" {
			arg = fmt.Sprintf("%s=%s", arg, v)
		}
		args = append(args, arg)
	}

	// Other flags.
	args = append(args, fmt.Sprintf("--access-points=%s", config.accessPoints))
	args = append(args, "--node-local-mount")
	args = append(args, "--v=5")

	klog.Infof("Successfully created mounter pod args: %+v", args)
	return args
}

func errorFilePath(stagingPath, podUID string) (string, error) {
	createEmptyDir := false
	emptyDirBasePath, err := util.PrepareEmptyDirForStagingPath(stagingPath, string(podUID), createEmptyDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(emptyDirBasePath, util.MounterPodErrorFile), nil
}

// waitForMounterServer waits for the pod to reach the Running phase and for the gRPC server to
// become fully operational by ensuring the socket file is available. It returns an error if the
// operation times out.
func (nm *nodeMounterManager) waitForMounterServer(ctx context.Context, podName, socketFile string) error {
	pollInterval := 1 * time.Second
	klog.Infof("Waiting for mounter pod %s/%s to start running and the mounter pod socket file %q to become available. Polling every %s",
		nm.mounterPodNamespace, podName, socketFile, pollInterval)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var pod *corev1.Pod
	var err error

	for {
		select {
		case <-ticker.C:
			// Check if the gRPC server socket file exists.
			if _, err = os.Stat(socketFile); err == nil {
				klog.Infof("Mounter pod %s/%s socket file %q is available.", nm.mounterPodNamespace, podName, socketFile)
				return nil
			}

			if !os.IsNotExist(err) {
				return fmt.Errorf("error checking socket file %q for mounter pod %s/%s: %w", socketFile, nm.mounterPodNamespace, podName, err)
			}

			klog.Infof("Mounter pod socket file %q not found. Checking mounter pod %s/%s status.", socketFile, nm.mounterPodNamespace, podName)
			// Get the current status of the mounter pod.
			pod, err = nm.mustGetMounterPod(podName)
			if err != nil {
				return err
			}
			if pod.Status.Phase == corev1.PodRunning {
				klog.Infof("Mounter pod %s/%s is running.", nm.mounterPodNamespace, podName)
				break
			}
			if pod.Status.Phase != corev1.PodPending {
				return status.Errorf(codes.Internal, "mounter pod %s/%s found in an unexpected status: %+v", nm.mounterPodNamespace, podName, pod.Status)
			}
			klog.Infof("Mounter pod %s/%s found with status: %+v. Waiting for pod to start running...", nm.mounterPodNamespace, podName, pod.Status)
		case <-ctx.Done():
			errMsg := fmt.Sprintf("timeout waiting for mounter pod %s/%s gRPC server to become available at %s",
				nm.mounterPodNamespace, podName, socketFile)
			if pod != nil {
				// The pod may be nil if the timeout is too sudden (e.g. less than a second)
				errMsg = fmt.Sprintf("%s, pod status: %+v", errMsg, pod.Status)
			}
			return status.Error(codes.DeadlineExceeded, errMsg)
		}
	}
}

// waitForMounterPodScheduled wait for mounter pod to be scheduled.
func (nm *nodeMounterManager) waitForMounterPodScheduled(ctx context.Context, podName string) error {
	pollInterval := 1 * time.Second
	klog.Infof("Waiting for mounter pod %s/%s to be scheduled. Polling every %q", nm.mounterPodNamespace, podName, pollInterval)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pod, err := nm.getMounterPod(podName)
			if err != nil {
				return err
			}
			if pod != nil {
				if pod.Spec.NodeName != "" {
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionTrue {
							klog.Infof("Mounter pod %s/%s has been scheduled to node %s", nm.mounterPodNamespace, podName, pod.Spec.NodeName)

							return nil
						}
					}
				}

				continue
			}
		case <-ctx.Done():
			errMsg := fmt.Sprintf("timeout waiting for mounter pod %s/%s to be scheduled", nm.mounterPodNamespace, podName)

			return status.Error(codes.DeadlineExceeded, errMsg)
		}
	}
}

// mustGetMounterPod returns the pod if it exists or returns an error if it doesn't.
// This function strictly expects the mounter pod to exist.
func (nm *nodeMounterManager) mustGetMounterPod(podName string) (*corev1.Pod, error) {
	pod, err := nm.getMounterPod(podName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pod == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "mounter pod %s/%s expected to exist but was not found", nm.mounterPodNamespace, podName)
	}
	return pod, nil
}
