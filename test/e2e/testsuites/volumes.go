/*
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testsuites

import (
	"fmt"

	"github.com/googlecloudplatform/gcs-fuse-csi-driver/test/e2e/specs"
	"github.com/onsi/ginkgo/v2"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

const mountPath = "/mnt/test"

type gcsFuseCSIVolumesTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitGcsFuseCSIVolumesTestSuite returns gcsFuseCSIVolumesTestSuite that implements TestSuite interface.
func InitGcsFuseCSIVolumesTestSuite() storageframework.TestSuite {
	return &gcsFuseCSIVolumesTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "volumes",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsCSIEphemeralVolume,
				storageframework.DefaultFsPreprovisionedPV,
				storageframework.DefaultFsDynamicPV,
			},
		},
	}
}

func (t *gcsFuseCSIVolumesTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *gcsFuseCSIVolumesTestSuite) SkipUnsupportedTests(_ storageframework.TestDriver, _ storageframework.TestPattern) {
}

func (t *gcsFuseCSIVolumesTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config         *storageframework.PerTestConfig
		volumeResource *storageframework.VolumeResource
	}
	var l local

	// Beware that it also registers an AfterEach which renders f unusable. Any code using
	// f must run inside an It or Context callback.
	f := framework.NewFrameworkWithCustomTimeouts("volumes", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	init := func(configPrefix ...string) {
		l = local{}
		l.config = driver.PrepareTest(f)
		if len(configPrefix) > 0 {
			l.config.Prefix = configPrefix[0]
		}
		l.volumeResource = storageframework.CreateVolumeResource(driver, l.config, pattern, e2evolume.SizeRange{})
	}

	cleanup := func() {
		var cleanUpErrs []error
		cleanUpErrs = append(cleanUpErrs, l.volumeResource.CleanupResource())
		err := utilerrors.NewAggregate(cleanUpErrs)
		framework.ExpectNoError(err, "while cleaning up")
	}

	ginkgo.It("should store data", func() {
		init()
		defer cleanup()

		ginkgo.By("Configuring the pod")
		tPod := specs.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetupVolume(l.volumeResource, "test-gcsfuse-volume", mountPath, false)

		ginkgo.By("Deploying the pod")
		tPod.Create()
		defer tPod.Cleanup()

		ginkgo.By("Checking that the pod is running")
		tPod.WaitForRunning()

		ginkgo.By("Checking that the pod command exits with no error")
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("mount | grep %v | grep rw,", mountPath))
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("echo 'hello world' > %v/data && grep 'hello world' %v/data", mountPath, mountPath))
	})

	ginkgo.It("[read-only] should fail when write", func() {
		init()
		defer cleanup()

		ginkgo.By("Configuring the writer pod")
		tPod := specs.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetName("gcsfuse-volume-tester-writer")
		tPod.SetupVolume(l.volumeResource, "test-gcsfuse-volume", mountPath, false)

		ginkgo.By("Deploying the writer pod")
		tPod.Create()

		ginkgo.By("Checking that the writer pod is running")
		tPod.WaitForRunning()

		ginkgo.By("Writing a file to the volume")
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("echo 'hello world' > %v/data && grep 'hello world' %v/data", mountPath, mountPath))

		ginkgo.By("Deleting the writer pod")
		tPod.Cleanup()

		ginkgo.By("Configuring the reader pod")
		tPod = specs.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetName("gcsfuse-volume-tester-reader")
		tPod.SetupVolume(l.volumeResource, "test-gcsfuse-volume", mountPath, true)

		ginkgo.By("Deploying the reader pod")
		tPod.Create()
		defer tPod.Cleanup()

		ginkgo.By("Checking that the reader pod is running")
		tPod.WaitForRunning()

		ginkgo.By("Checking that the reader pod command exits with no error")
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("mount | grep %v | grep ro,", mountPath))
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("grep 'hello world' %v/data", mountPath))

		ginkgo.By("Expecting error when write to read-only volumes")
		tPod.VerifyExecInPodFail(f, specs.TesterContainerName, fmt.Sprintf("echo 'hello world' > %v/data", mountPath), 1)
	})

	ginkgo.It("[non-root] should store data", func() {
		init(specs.NonRootVolumePrefix)
		defer cleanup()

		ginkgo.By("Configuring the pod")
		tPod := specs.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetNonRootSecurityContext()
		tPod.SetupVolume(l.volumeResource, "test-gcsfuse-volume", mountPath, false)

		ginkgo.By("Deploying the pod")
		tPod.Create()
		defer tPod.Cleanup()

		ginkgo.By("Checking that the pod is running")
		tPod.WaitForRunning()

		ginkgo.By("Checking that the pod command exits with no error")
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("mount | grep %v | grep rw,", mountPath))
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("echo 'hello world' > %v/data && grep 'hello world' %v/data", mountPath, mountPath))
	})

	ginkgo.It("should store data in implicit directory", func() {
		if pattern.VolType == storageframework.DynamicPV {
			e2eskipper.Skipf("skip for volume type %v", storageframework.DynamicPV)
		}

		init(specs.ImplicitDirsVolumePrefix)
		defer cleanup()

		ginkgo.By("Configuring the pod")
		tPod := specs.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetupVolume(l.volumeResource, "test-gcsfuse-volume", mountPath, false)

		ginkgo.By("Deploying the pod")
		tPod.Create()
		defer tPod.Cleanup()

		ginkgo.By("Checking that the pod is running")
		tPod.WaitForRunning()

		ginkgo.By("Checking that the pod command exits with no error")
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("mount | grep %v | grep rw,", mountPath))
		tPod.VerifyExecInPodSucceed(f, specs.TesterContainerName, fmt.Sprintf("echo 'hello world' > %v/%v/data && grep 'hello world' %v/%v/data", mountPath, specs.ImplicitDirsPath, mountPath, specs.ImplicitDirsPath))
	})
}
