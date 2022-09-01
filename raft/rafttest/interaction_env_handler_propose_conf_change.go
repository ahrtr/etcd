// Copyright 2019 The etcd Authors
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

package rafttest

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (env *InteractionEnv) handleProposeConfChange(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	var v1 bool
	transition := raftpb.ConfChangeTransitionAuto
	for _, arg := range d.CmdArgs[1:] {
		for _, val := range arg.Vals {
			switch arg.Key {
			case "v1":
				var err error
				v1, err = strconv.ParseBool(val)
				if err != nil {
					return err
				}
			case "transition":
				switch val {
				case "auto":
					transition = raftpb.ConfChangeTransitionAuto
				case "implicit":
					transition = raftpb.ConfChangeTransitionJointImplicit
				case "explicit":
					transition = raftpb.ConfChangeTransitionJointExplicit
				default:
					return fmt.Errorf("unknown transition %s", val)
				}
			default:
				return fmt.Errorf("unknown command %s", arg.Key)
			}
		}
	}

	ccs, err := raftpb.ConfChangesFromString(d.Input)
	if err != nil {
		return err
	}

	var c raftpb.ConfChangeI
	if v1 {
		if len(ccs) > 1 || transition != raftpb.ConfChangeTransitionAuto {
			return fmt.Errorf("v1 conf change can only have one operation and no transition")
		}
		c = raftpb.ConfChange{
			Type:   ccs[0].Type,
			NodeID: ccs[0].NodeID,
		}
	} else {
		c = raftpb.ConfChangeV2{
			Transition: transition,
			Changes:    ccs,
		}
	}
	if err := env.ProposeConfChange(idx, c); err != nil {
		return err
	}

	return env.ReportStatus(idx)
}

// ProposeConfChange proposes a configuration change on the node with the given index.
func (env *InteractionEnv) ProposeConfChange(idx int, c raftpb.ConfChangeI) error {
	return env.Nodes[idx].ProposeConfChange(c)
}

func (env *InteractionEnv) ReportStatus(idx int) error {
	rn := env.Nodes[idx]
	rd := rn.Ready()
	status := rn.Status()

	if status.ID == status.Lead && (len(rd.Entries) > 1 || (len(rd.Entries) == 1 && len(rd.Entries[0].Data) > 0)) {
		e := rd.Entries[len(rd.Entries)-1]
		return rn.Step(raftpb.Message{From: status.ID, To: status.ID, Type: raftpb.MsgAppResp, Index: e.Index})
	}

	return nil
}
