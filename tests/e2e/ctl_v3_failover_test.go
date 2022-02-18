// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"testing"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

func TestFailover1ClientTLS(t *testing.T) { testCtl(t, getTest, withCfg(*e2e.NewConfigClientTLS())) }

func failover1(cx ctlCtx) {
	key, value := "foo", "bar"

	if err := ctlV3Put(cx, key, value, ""); err != nil {
		if cx.dialTimeout > 0 && !isGRPCTimedout(err) {
			cx.t.Fatalf("putTest ctlV3Put error (%v)", err)
		}
	}
	if err := ctlV3Get(cx, []string{key}, kv{key, value}); err != nil {
		if cx.dialTimeout > 0 && !isGRPCTimedout(err) {
			cx.t.Fatalf("putTest ctlV3Get error (%v)", err)
		}
	}
}
