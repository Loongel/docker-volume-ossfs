package secret // import "github.com/docker/docker/integration/secret"

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	swarmtypes "github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
	"github.com/docker/docker/integration/internal/swarm"
	"github.com/docker/docker/pkg/stdcopy"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
	"gotest.tools/skip"
)

func TestSecretInspect(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType == "windows")

	defer setupTest(t)()
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()

	ctx := context.Background()

	testName := "test_secret_" + t.Name()
	secretID := createSecret(ctx, t, client, testName, []byte("TESTINGDATA"), nil)

	secret, _, err := client.SecretInspectWithRaw(context.Background(), secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(secret.Spec.Name, testName))

	secret, _, err = client.SecretInspectWithRaw(context.Background(), testName)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(secretID, secretID))
}

func TestSecretList(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType == "windows")

	defer setupTest(t)()
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()
	ctx := context.Background()

	testName0 := "test0_" + t.Name()
	testName1 := "test1_" + t.Name()
	testNames := []string{testName0, testName1}
	sort.Strings(testNames)

	// create secret test0
	createSecret(ctx, t, client, testName0, []byte("TESTINGDATA0"), map[string]string{"type": "test"})

	// create secret test1
	secret1ID := createSecret(ctx, t, client, testName1, []byte("TESTINGDATA1"), map[string]string{"type": "production"})

	// test by `secret ls`
	entries, err := client.SecretList(ctx, types.SecretListOptions{})
	assert.NilError(t, err)
	assert.Check(t, is.DeepEqual(secretNamesFromList(entries), testNames))

	testCases := []struct {
		filters  filters.Args
		expected []string
	}{
		// test filter by name `secret ls --filter name=xxx`
		{
			filters:  filters.NewArgs(filters.Arg("name", testName0)),
			expected: []string{testName0},
		},
		// test filter by id `secret ls --filter id=xxx`
		{
			filters:  filters.NewArgs(filters.Arg("id", secret1ID)),
			expected: []string{testName1},
		},
		// test filter by label `secret ls --filter label=xxx`
		{
			filters:  filters.NewArgs(filters.Arg("label", "type")),
			expected: testNames,
		},
		{
			filters:  filters.NewArgs(filters.Arg("label", "type=test")),
			expected: []string{testName0},
		},
		{
			filters:  filters.NewArgs(filters.Arg("label", "type=production")),
			expected: []string{testName1},
		},
	}
	for _, tc := range testCases {
		entries, err = client.SecretList(ctx, types.SecretListOptions{
			Filters: tc.filters,
		})
		assert.NilError(t, err)
		assert.Check(t, is.DeepEqual(secretNamesFromList(entries), tc.expected))

	}
}

func createSecret(ctx context.Context, t *testing.T, client client.APIClient, name string, data []byte, labels map[string]string) string {
	secret, err := client.SecretCreate(ctx, swarmtypes.SecretSpec{
		Annotations: swarmtypes.Annotations{
			Name:   name,
			Labels: labels,
		},
		Data: data,
	})
	assert.NilError(t, err)
	assert.Check(t, secret.ID != "")
	return secret.ID
}

func TestSecretsCreateAndDelete(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType == "windows")

	defer setupTest(t)()
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()
	ctx := context.Background()

	testName := "test_secret_" + t.Name()
	secretID := createSecret(ctx, t, client, testName, []byte("TESTINGDATA"), nil)

	// create an already existin secret, daemon should return a status code of 409
	_, err := client.SecretCreate(ctx, swarmtypes.SecretSpec{
		Annotations: swarmtypes.Annotations{
			Name: testName,
		},
		Data: []byte("TESTINGDATA"),
	})
	assert.Check(t, is.ErrorContains(err, "already exists"))

	// Ported from original TestSecretsDelete
	err = client.SecretRemove(ctx, secretID)
	assert.NilError(t, err)

	_, _, err = client.SecretInspectWithRaw(ctx, secretID)
	assert.Check(t, is.ErrorContains(err, "No such secret"))

	err = client.SecretRemove(ctx, "non-existin")
	assert.Check(t, is.ErrorContains(err, "No such secret: non-existin"))

	// Ported from original TestSecretsCreteaWithLabels
	testName = "test_secret_with_labels_" + t.Name()
	secretID = createSecret(ctx, t, client, testName, []byte("TESTINGDATA"), map[string]string{
		"key1": "value1",
		"key2": "value2",
	})

	insp, _, err := client.SecretInspectWithRaw(ctx, secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insp.Spec.Name, testName))
	assert.Check(t, is.Equal(len(insp.Spec.Labels), 2))
	assert.Check(t, is.Equal(insp.Spec.Labels["key1"], "value1"))
	assert.Check(t, is.Equal(insp.Spec.Labels["key2"], "value2"))
}

func TestSecretsUpdate(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType == "windows")

	defer setupTest(t)()
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()
	ctx := context.Background()

	testName := "test_secret_" + t.Name()
	secretID := createSecret(ctx, t, client, testName, []byte("TESTINGDATA"), nil)

	insp, _, err := client.SecretInspectWithRaw(ctx, secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insp.ID, secretID))

	// test UpdateSecret with full ID
	insp.Spec.Labels = map[string]string{"test": "test1"}
	err = client.SecretUpdate(ctx, secretID, insp.Version, insp.Spec)
	assert.NilError(t, err)

	insp, _, err = client.SecretInspectWithRaw(ctx, secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insp.Spec.Labels["test"], "test1"))

	// test UpdateSecret with full name
	insp.Spec.Labels = map[string]string{"test": "test2"}
	err = client.SecretUpdate(ctx, testName, insp.Version, insp.Spec)
	assert.NilError(t, err)

	insp, _, err = client.SecretInspectWithRaw(ctx, secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insp.Spec.Labels["test"], "test2"))

	// test UpdateSecret with prefix ID
	insp.Spec.Labels = map[string]string{"test": "test3"}
	err = client.SecretUpdate(ctx, secretID[:1], insp.Version, insp.Spec)
	assert.NilError(t, err)

	insp, _, err = client.SecretInspectWithRaw(ctx, secretID)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insp.Spec.Labels["test"], "test3"))

	// test UpdateSecret in updating Data which is not supported in daemon
	// this test will produce an error in func UpdateSecret
	insp.Spec.Data = []byte("TESTINGDATA2")
	err = client.SecretUpdate(ctx, secretID, insp.Version, insp.Spec)
	assert.Check(t, is.ErrorContains(err, "only updates to Labels are allowed"))
}

func TestTemplatedSecret(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType == "windows")
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()
	ctx := context.Background()

	referencedSecretName := "referencedsecret_" + t.Name()
	referencedSecretSpec := swarmtypes.SecretSpec{
		Annotations: swarmtypes.Annotations{
			Name: referencedSecretName,
		},
		Data: []byte("this is a secret"),
	}
	referencedSecret, err := client.SecretCreate(ctx, referencedSecretSpec)
	assert.Check(t, err)

	referencedConfigName := "referencedconfig_" + t.Name()
	referencedConfigSpec := swarmtypes.ConfigSpec{
		Annotations: swarmtypes.Annotations{
			Name: referencedConfigName,
		},
		Data: []byte("this is a config"),
	}
	referencedConfig, err := client.ConfigCreate(ctx, referencedConfigSpec)
	assert.Check(t, err)

	templatedSecretName := "templated_secret_" + t.Name()
	secretSpec := swarmtypes.SecretSpec{
		Annotations: swarmtypes.Annotations{
			Name: templatedSecretName,
		},
		Templating: &swarmtypes.Driver{
			Name: "golang",
		},
		Data: []byte("SERVICE_NAME={{.Service.Name}}\n" +
			"{{secret \"referencedsecrettarget\"}}\n" +
			"{{config \"referencedconfigtarget\"}}\n"),
	}

	templatedSecret, err := client.SecretCreate(ctx, secretSpec)
	assert.Check(t, err)

	serviceName := "svc_" + t.Name()
	serviceID := swarm.CreateService(t, d,
		swarm.ServiceWithSecret(
			&swarmtypes.SecretReference{
				File: &swarmtypes.SecretReferenceFileTarget{
					Name: "templated_secret",
					UID:  "0",
					GID:  "0",
					Mode: 0600,
				},
				SecretID:   templatedSecret.ID,
				SecretName: templatedSecretName,
			},
		),
		swarm.ServiceWithConfig(
			&swarmtypes.ConfigReference{
				File: &swarmtypes.ConfigReferenceFileTarget{
					Name: "referencedconfigtarget",
					UID:  "0",
					GID:  "0",
					Mode: 0600,
				},
				ConfigID:   referencedConfig.ID,
				ConfigName: referencedConfigName,
			},
		),
		swarm.ServiceWithSecret(
			&swarmtypes.SecretReference{
				File: &swarmtypes.SecretReferenceFileTarget{
					Name: "referencedsecrettarget",
					UID:  "0",
					GID:  "0",
					Mode: 0600,
				},
				SecretID:   referencedSecret.ID,
				SecretName: referencedSecretName,
			},
		),
		swarm.ServiceWithName(serviceName),
	)

	var tasks []swarmtypes.Task
	waitAndAssert(t, 60*time.Second, func(t *testing.T) bool {
		tasks = swarm.GetRunningTasks(t, client, serviceID)
		return len(tasks) > 0
	})

	task := tasks[0]
	waitAndAssert(t, 60*time.Second, func(t *testing.T) bool {
		if task.NodeID == "" || (task.Status.ContainerStatus == nil || task.Status.ContainerStatus.ContainerID == "") {
			task, _, _ = client.TaskInspectWithRaw(context.Background(), task.ID)
		}
		return task.NodeID != "" && task.Status.ContainerStatus != nil && task.Status.ContainerStatus.ContainerID != ""
	})

	attach := swarm.ExecTask(t, d, task, types.ExecConfig{
		Cmd:          []string{"/bin/cat", "/run/secrets/templated_secret"},
		AttachStdout: true,
		AttachStderr: true,
	})

	expect := "SERVICE_NAME=" + serviceName + "\n" +
		"this is a secret\n" +
		"this is a config\n"
	assertAttachedStream(t, attach, expect)

	attach = swarm.ExecTask(t, d, task, types.ExecConfig{
		Cmd:          []string{"mount"},
		AttachStdout: true,
		AttachStderr: true,
	})
	assertAttachedStream(t, attach, "tmpfs on /run/secrets/templated_secret type tmpfs")
}

// Test case for 28884
func TestSecretCreateResolve(t *testing.T) {
	skip.If(t, testEnv.DaemonInfo.OSType != "linux")

	defer setupTest(t)()
	d := swarm.NewSwarm(t, testEnv)
	defer d.Stop(t)
	client := d.NewClientT(t)
	defer client.Close()

	ctx := context.Background()

	testName := "test_secret_" + t.Name()
	secretID := createSecret(ctx, t, client, testName, []byte("foo"), nil)

	fakeName := secretID
	fakeID := createSecret(ctx, t, client, fakeName, []byte("fake foo"), nil)

	entries, err := client.SecretList(ctx, types.SecretListOptions{})
	assert.NilError(t, err)
	assert.Check(t, is.Contains(secretNamesFromList(entries), testName))
	assert.Check(t, is.Contains(secretNamesFromList(entries), fakeName))

	err = client.SecretRemove(ctx, secretID)
	assert.NilError(t, err)

	// Fake one will remain
	entries, err = client.SecretList(ctx, types.SecretListOptions{})
	assert.NilError(t, err)
	assert.Assert(t, is.DeepEqual(secretNamesFromList(entries), []string{fakeName}))

	// Remove based on name prefix of the fake one should not work
	// as search is only done based on:
	// - Full ID
	// - Full Name
	// - Partial ID (prefix)
	err = client.SecretRemove(ctx, fakeName[:5])
	assert.Assert(t, nil != err)
	entries, err = client.SecretList(ctx, types.SecretListOptions{})
	assert.NilError(t, err)
	assert.Assert(t, is.DeepEqual(secretNamesFromList(entries), []string{fakeName}))

	// Remove based on ID prefix of the fake one should succeed
	err = client.SecretRemove(ctx, fakeID[:5])
	assert.NilError(t, err)
	entries, err = client.SecretList(ctx, types.SecretListOptions{})
	assert.NilError(t, err)
	assert.Assert(t, is.Equal(0, len(entries)))
}

func assertAttachedStream(t *testing.T, attach types.HijackedResponse, expect string) {
	buf := bytes.NewBuffer(nil)
	_, err := stdcopy.StdCopy(buf, buf, attach.Reader)
	assert.NilError(t, err)
	assert.Check(t, is.Contains(buf.String(), expect))
}

func waitAndAssert(t *testing.T, timeout time.Duration, f func(*testing.T) bool) {
	t.Helper()
	after := time.After(timeout)
	for {
		select {
		case <-after:
			t.Fatal("timed out waiting for condition")
		default:
		}
		if f(t) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func secretNamesFromList(entries []swarmtypes.Secret) []string {
	var values []string
	for _, entry := range entries {
		values = append(values, entry.Spec.Name)
	}
	sort.Strings(values)
	return values
}
