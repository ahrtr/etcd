package v3discovery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/client/v3"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
)

var (
	clusterFakeToken = "fakeToken"

	actualMemberInfo = []memberInfo{
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(101).String(),
			peerURLsMap: "infra1=http://192.168.0.100:2380",
			createRev:   8,
		},
		{
			// invalid peer registry key
			peerRegKey:  "/invalidPrefix/fakeToken/members/" + types.ID(102).String(),
			peerURLsMap: "infra2=http://192.168.0.102:2380",
			createRev:   6,
		},
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(102).String(),
			peerURLsMap: "infra2=http://192.168.0.102:2380",
			createRev:   6,
		},
		{
			// invalid peer info format
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(102).String(),
			peerURLsMap: "http://192.168.0.102:2380",
			createRev:   6,
		},
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(103).String(),
			peerURLsMap: "infra3=http://192.168.0.103:2380",
			createRev:   7,
		},
		{
			// duplicate peer
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(101).String(),
			peerURLsMap: "infra1=http://192.168.0.100:2380",
			createRev:   2,
		},
	}

	// sort by CreateRevision
	expectedMemberInfo = []memberInfo{
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(102).String(),
			peerURLsMap: "infra2=http://192.168.0.102:2380",
			createRev:   6,
		},
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(103).String(),
			peerURLsMap: "infra3=http://192.168.0.103:2380",
			createRev:   7,
		},
		{
			peerRegKey:  "/_etcd/registry/fakeToken/members/" + types.ID(101).String(),
			peerURLsMap: "infra1=http://192.168.0.100:2380",
			createRev:   8,
		},
	}
)

// fakeKVForClusterSize is used to test getClusterSize.
type fakeKVForClusterSize struct {
	*fakeBaseKV
	clusterSize string
}

// We only need to overwrite the method `Get`.
func (fkv *fakeKVForClusterSize) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if fkv.clusterSize == "" {
		// cluster size isn't configured in this case
		return &clientv3.GetResponse{}, nil
	}

	return &clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{
			{
				Value: []byte(fkv.clusterSize),
			},
		},
	}, nil
}

func TestGetClusterSize(t *testing.T) {
	cases := []struct {
		clusterSize  string
		expectedErr  error
		expectedSize int
	}{
		{
			clusterSize: "",
			expectedErr: ErrSizeNotFound,
		},
		{
			clusterSize: "invalidSize",
			expectedErr: ErrBadSizeKey,
		},
		{
			clusterSize:  "3",
			expectedErr:  nil,
			expectedSize: 3,
		},
	}

	lg, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create a logger, error: %v", err)
	}
	for i, tc := range cases {
		d := &discovery{
			lg: lg,
			c: &clientv3.Client{
				KV: &fakeKVForClusterSize{
					fakeBaseKV:  &fakeBaseKV{},
					clusterSize: tc.clusterSize,
				},
			},
			cfg:          &DiscoveryConfig{},
			clusterToken: clusterFakeToken,
		}

		if cs, err := d.getClusterSize(); err != tc.expectedErr {
			t.Errorf("case %d failed, expected error: %v got: %v", i, tc.expectedErr, err)
		} else {
			if err == nil && cs != tc.expectedSize {
				t.Errorf("case %d failed, expected clusterSize: %d got: %d", i, tc.expectedSize, cs)
			}
		}
	}
}

// fakeKVForClusterMembers is used to test getClusterMembers.
type fakeKVForClusterMembers struct {
	*fakeBaseKV
	members []memberInfo
}

// We only need to overwrite method `Get`.
func (fkv *fakeKVForClusterMembers) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	kvs := memberInfoToKeyValues(fkv.members)

	return &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: 10,
		},
		Kvs: kvs,
	}, nil
}

func memberInfoToKeyValues(members []memberInfo) []*mvccpb.KeyValue {
	kvs := make([]*mvccpb.KeyValue, 0)
	for _, mi := range members {
		kvs = append(kvs, &mvccpb.KeyValue{
			Key:            []byte(mi.peerRegKey),
			Value:          []byte(mi.peerURLsMap),
			CreateRevision: mi.createRev,
		})
	}

	return kvs
}

func TestGetClusterMembers(t *testing.T) {
	lg, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create a logger, error: %v", err)
	}

	d := &discovery{
		lg: lg,
		c: &clientv3.Client{
			KV: &fakeKVForClusterMembers{
				fakeBaseKV: &fakeBaseKV{},
				members:    actualMemberInfo,
			},
		},
		cfg:          &DiscoveryConfig{},
		clusterToken: clusterFakeToken,
	}

	clsInfo, _, err := d.getClusterMembers()
	if err != nil {
		t.Errorf("Failed to get cluster members, error: %v", err)
	}

	if clsInfo.Len() != len(expectedMemberInfo) {
		t.Errorf("unexpected member count, expected: %d, got: %d", len(expectedMemberInfo), clsInfo.Len())
	}

	for i, m := range clsInfo.members {
		if m != expectedMemberInfo[i] {
			t.Errorf("unexpected member[%d], expected: %v, got: %v", i, expectedMemberInfo[i], m)
		}
	}
}

// fakeKVForCheckCluster is used to test checkCluster.
type fakeKVForCheckCluster struct {
	*fakeBaseKV
	t                 *testing.T
	token             string
	clusterSize       string
	members           []memberInfo
	getSizeRetries    int
	getMembersRetries int
}

// We only need to overwrite method `Get`.
func (fkv *fakeKVForCheckCluster) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	clusterSizeKey := fmt.Sprintf("/_etcd/registry/%s/_config/size", fkv.token)
	clusterMembersKey := fmt.Sprintf("/_etcd/registry/%s/members", fkv.token)

	if key == clusterSizeKey {
		if fkv.getSizeRetries > 0 {
			fkv.getSizeRetries--
			// discovery client should retry on error.
			return nil, errors.New("get cluster size failed")
		}
		return &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Value: []byte(fkv.clusterSize),
				},
			},
		}, nil

	} else if key == clusterMembersKey {
		if fkv.getMembersRetries > 0 {
			fkv.getMembersRetries--
			// discovery client should retry on error.
			return nil, errors.New("get cluster members failed")
		}
		kvs := memberInfoToKeyValues(fkv.members)

		return &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{
				Revision: 10,
			},
			Kvs: kvs,
		}, nil
	} else {
		fkv.t.Errorf("unexpected key: %s", key)
		return nil, fmt.Errorf("unexpected key: %s", key)
	}
}

func TestCheckCluster(t *testing.T) {
	cases := []struct {
		memberId          string
		getSizeRetries    int
		getMembersRetries int
		expectedError     error
	}{
		{
			memberId:          types.ID(101).String(),
			getSizeRetries:    0,
			getMembersRetries: 0,
			expectedError:     nil,
		},
		{
			memberId:          types.ID(102).String(),
			getSizeRetries:    2,
			getMembersRetries: 0,
			expectedError:     nil,
		},
		{
			memberId:          types.ID(103).String(),
			getSizeRetries:    0,
			getMembersRetries: 2,
			expectedError:     nil,
		},
		{
			memberId:          types.ID(104).String(),
			getSizeRetries:    0,
			getMembersRetries: 0,
			expectedError:     ErrFullCluster,
		},
	}

	for i, tc := range cases {
		lg, err := zap.NewProduction()
		if err != nil {
			t.Errorf("Failed to create a logger, error: %v", err)
		}

		fkv := &fakeKVForCheckCluster{
			fakeBaseKV:        &fakeBaseKV{},
			t:                 t,
			token:             clusterFakeToken,
			clusterSize:       "3",
			members:           actualMemberInfo,
			getSizeRetries:    tc.getSizeRetries,
			getMembersRetries: tc.getMembersRetries,
		}

		d := &discovery{
			lg: lg,
			c: &clientv3.Client{
				KV: fkv,
			},
			cfg:          &DiscoveryConfig{},
			clusterToken: clusterFakeToken,
			clock:        clockwork.NewRealClock(),
		}

		clsInfo, _, _, err := d.checkCluster()
		if err != tc.expectedError {
			t.Errorf("case %d failed, expected error: %v, got: %v", i, tc.expectedError, err)
		}

		if err == nil {
			if fkv.getSizeRetries != 0 || fkv.getMembersRetries != 0 {
				t.Errorf("case %d failed, discovery client did not retry checking cluster on error, remaining etries: (%d, %d)", i, fkv.getSizeRetries, fkv.getMembersRetries)
			}

			if clsInfo.Len() != len(expectedMemberInfo) {
				t.Errorf("case %d failed, unexpected member count, expected: %d, got: %d", len(expectedMemberInfo), i, clsInfo.Len())
			}

			for mIdx, m := range clsInfo.members {
				if m != expectedMemberInfo[mIdx] {
					t.Errorf("case %d failed, unexpected member[%d], expected: %v, got: %v", i, mIdx, expectedMemberInfo[mIdx], m)
				}
			}
		}
	}
}

func TestRegisterSelf(t *testing.T) {
	cases := []struct {
		token            string
		memberId         types.ID
		expectedRegKey   string
		expectedRegValue string
		retries          int // when retries > 0, then return an error on Put request.
	}{
		{
			token:            "token1",
			memberId:         101,
			expectedRegKey:   "/_etcd/registry/token1/members/" + types.ID(101).String(),
			expectedRegValue: "infra=http://127.0.0.1:2380",
			retries:          0,
		},
		{
			token:            "token2",
			memberId:         102,
			expectedRegKey:   "/_etcd/registry/token2/members/" + types.ID(102).String(),
			expectedRegValue: "infra=http://127.0.0.1:2380",
			retries:          0,
		},
		{
			token:            "token3",
			memberId:         103,
			expectedRegKey:   "/_etcd/registry/token3/members/" + types.ID(103).String(),
			expectedRegValue: "infra=http://127.0.0.1:2380",
			retries:          2,
		},
	}

	lg, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create a logger, error: %v", err)
	}

	for i, tc := range cases {
		d := &discovery{
			lg:           lg,
			clusterToken: tc.token,
			memberId:     tc.memberId,
			cfg:          &DiscoveryConfig{},
			c: &clientv3.Client{
				KV: &fakeBaseKV{
					// define the putMethod here, so that it can access all the variable in the closure environment directly.
					putMethod: func(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
						if key != tc.expectedRegKey {
							t.Errorf("case %d failed, unexpected register key, expected: %s, got: %s", i, tc.expectedRegKey, key)
						}

						if val != tc.expectedRegValue {
							t.Errorf("case %d failed, unexpected register value, expected: %s, got: %s", i, tc.expectedRegValue, val)
						}

						if tc.retries <= 0 {
							return nil, nil
						}

						tc.retries--
						return nil, errors.New("register self failed")
					},
				},
			},
			clock: clockwork.NewRealClock(),
		}

		if err := d.registerSelf(tc.expectedRegValue); err != nil {
			t.Errorf("failed to register member self, error: %v", err)
		}

		if tc.retries != 0 {
			t.Errorf("discovery client did not retry registering itself on error, remaining retries: %d", tc.retries)
		}
	}
}

// fakeWatcherForWaitPeers is used to test waitPeers.
type fakeWatcherForWaitPeers struct {
	*fakeBaseWatcher
	t     *testing.T
	token string
}

// We only need to overwrite method `Watch`.
func (fw *fakeWatcherForWaitPeers) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	expectedWatchKey := fmt.Sprintf("/_etcd/registry/%s/members", fw.token)
	if key != expectedWatchKey {
		fw.t.Errorf("unexpected watch key, expected: %s, got: %s", expectedWatchKey, key)
	}

	ch := make(chan clientv3.WatchResponse, 1)
	go func() {
		for _, mi := range actualMemberInfo {
			ch <- clientv3.WatchResponse{
				Events: []*clientv3.Event{
					{
						Kv: &mvccpb.KeyValue{
							Key:            []byte(mi.peerRegKey),
							Value:          []byte(mi.peerURLsMap),
							CreateRevision: mi.createRev,
						},
					},
				},
			}
		}
		close(ch)
	}()
	return ch
}

func TestWaitPeers(t *testing.T) {
	lg, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create a logger, error: %v", err)
	}

	d := &discovery{
		lg: lg,
		c: &clientv3.Client{
			KV: &fakeKVForClusterMembers{
				fakeBaseKV: &fakeBaseKV{},
				members:    actualMemberInfo,
			},
			Watcher: &fakeWatcherForWaitPeers{
				fakeBaseWatcher: &fakeBaseWatcher{},
				t:               t,
				token:           clusterFakeToken,
			},
		},
		cfg:          &DiscoveryConfig{},
		clusterToken: clusterFakeToken,
	}

	cls := clusterInfo{
		clusterToken: clusterFakeToken,
	}

	d.waitPeers(&cls, 3, 0)

	if cls.Len() != len(expectedMemberInfo) {
		t.Errorf("unexpected member number returned by watch, expected: %d, got: %d", len(expectedMemberInfo), cls.Len())
	}

	for i, m := range cls.members {
		if m != expectedMemberInfo[i] {
			t.Errorf("unexpected member[%d] returned by watch, expected: %v, got: %v", i, expectedMemberInfo[i], m)
		}
	}
}

// fakeBaseKV is the base struct implementing the interface `clientv3.KV`.
type fakeBaseKV struct {
	putMethod func(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
}

func (fkv *fakeBaseKV) Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if fkv.putMethod != nil {
		return fkv.putMethod(ctx, key, val, opts...)
	}
	return nil, nil
}

func (fkv *fakeBaseKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, nil
}

func (fkv *fakeBaseKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, nil
}

func (fkv *fakeBaseKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}

func (fkv *fakeBaseKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (fkv *fakeBaseKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

// fakeBaseWatcher is the base struct implementing the interface `clientv3.Watcher`.
type fakeBaseWatcher struct{}

func (fw *fakeBaseWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return nil
}

func (fw *fakeBaseWatcher) RequestProgress(ctx context.Context) error {
	return nil
}

func (fw *fakeBaseWatcher) Close() error {
	return nil
}
