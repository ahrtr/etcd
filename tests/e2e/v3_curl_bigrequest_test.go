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
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

func TestV3CurlPut_BigRequest_NoTLS_Success(t *testing.T) {
	testCtl(t, testV3CurlBigRequestSuccess, withCfg(*e2e.NewConfigNoTLS()), withMaxRequestBytes(256*1024))
}

func TestV3CurlPut_BigRequest_TLS_Success(t *testing.T) {
	testCtl(t, testV3CurlBigRequestSuccess, withCfg(*e2e.NewConfigTLS()), withMaxRequestBytes(256*1024))
}

func TestV3CurlPut_BigRequest_NoTLS_Fail(t *testing.T) {
	testCtl(t, testV3CurlBigRequestFail, withCfg(*e2e.NewConfigNoTLS()), withMaxRequestBytes(256*1024))
}

func TestV3CurlPut_BigRequest_TLS_Fail(t *testing.T) {
	testCtl(t, testV3CurlBigRequestFail, withCfg(*e2e.NewConfigTLS()), withMaxRequestBytes(256*1024))
}

func testV3CurlBigRequestSuccess(cx ctlCtx) {
	overhead := 512
	dataSize := int(cx.cfg.MaxRequestBytes)*3/4 - overhead

	data := generateBigRequestData(cx.t, dataSize)
	testV3CurlBigRequest(cx, data, true)
}

func testV3CurlBigRequestFail(cx ctlCtx) {
	overhead := 512
	dataSize := int(cx.cfg.MaxRequestBytes) + overhead

	data := generateBigRequestData(cx.t, dataSize)
	testV3CurlBigRequest(cx, data, false)
}

func testV3CurlBigRequest(cx ctlCtx, data []byte, success bool) {
	var (
		key   = []byte("abcd")
		value = data // this will be automatically base64-encoded by Go

		expectPut = `"revision":"`
		expectGet = `"value":"`
	)
	putData, err := json.Marshal(&pb.PutRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		cx.t.Fatal(err)
	}

	rangeData, err := json.Marshal(&pb.RangeRequest{
		Key: key,
	})
	if err != nil {
		cx.t.Fatal(err)
	}

	cx.t.Logf("Sending put request, len(putData): %d", len(putData))
	err = e2e.CURLPost(cx.epc, e2e.CURLReq{Endpoint: "/v3/kv/put", Value: string(putData), Expected: expectPut})
	if err != nil {
		if success {
			cx.t.Fatalf("testV3CurlBigRequest put failed, error: %v", err)
		} else {
			assert.Contains(cx.t, err.Error(), "request is too large")
		}
	} else {
		if !success {
			cx.t.Fatal("Expected put failure, but successful.")
		}
	}

	if success {
		if err := e2e.CURLPost(cx.epc, e2e.CURLReq{Endpoint: "/v3/kv/range", Value: string(rangeData), Expected: expectGet}); err != nil {
			cx.t.Fatalf("testV3CurlBigRequest get failed, error: %v", err)
		}
	}
}

func generateBigRequestData(t *testing.T, size int) []byte {
	t.Logf("Generating data of size: %d\n", size)
	data := make([]byte, size)
	n, err := rand.Read(data)
	assert.Equal(t, size, n)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	return data
}
