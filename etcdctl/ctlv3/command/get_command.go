// Copyright 2015 The etcd Authors
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

package command

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/cobrautl"
)

var (
	getConsistency  string
	getLimit        int64
	getSortOrder    string
	getSortTarget   string
	getPrefix       bool
	getFromKey      bool
	getRev          int64
	getKeysOnly     bool
	getCountOnly    bool
	printValueOnly  bool
	getMinCreateRev int64
	getMaxCreateRev int64
	getMinModRev    int64
	getMaxModRev    int64
)

// NewGetCommand returns the cobra command for "get".
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [options] <key> [range_end]",
		Short: "Gets the key or a range of keys",
		Run:   getCommandFunc,
	}

	cmd.Flags().StringVar(&getConsistency, "consistency", "l", "Linearizable(l) or Serializable(s)")
	cmd.Flags().StringVar(&getSortOrder, "order", "", "Order of results; ASCEND or DESCEND (ASCEND by default)")
	cmd.Flags().StringVar(&getSortTarget, "sort-by", "", "Sort target; CREATE, KEY, MODIFY, VALUE, or VERSION")
	cmd.Flags().Int64Var(&getLimit, "limit", 0, "Maximum number of results")
	cmd.Flags().BoolVar(&getPrefix, "prefix", false, "Get keys with matching prefix")
	cmd.Flags().BoolVar(&getFromKey, "from-key", false, "Get keys that are greater than or equal to the given key using byte compare")
	cmd.Flags().Int64Var(&getRev, "rev", 0, "Specify the kv revision")
	cmd.Flags().BoolVar(&getKeysOnly, "keys-only", false, "Get only the keys")
	cmd.Flags().BoolVar(&getCountOnly, "count-only", false, "Get only the count")
	cmd.Flags().BoolVar(&printValueOnly, "print-value-only", false, `Only write values when using the "simple" output format`)
	cmd.Flags().Int64Var(&getMinCreateRev, "min-create-rev", 0, "Minimum create revision")
	cmd.Flags().Int64Var(&getMaxCreateRev, "max-create-rev", 0, "Maximum create revision")
	cmd.Flags().Int64Var(&getMinModRev, "min-mod-rev", 0, "Minimum modification revision")
	cmd.Flags().Int64Var(&getMaxModRev, "max-mod-rev", 0, "Maximum modification revision")

	cmd.RegisterFlagCompletionFunc("consistency", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"l", "s"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("order", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"ASCEND", "DESCEND"}, cobra.ShellCompDirectiveDefault
	})
	cmd.RegisterFlagCompletionFunc("sort-by", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"CREATE", "KEY", "MODIFY", "VALUE", "VERSION"}, cobra.ShellCompDirectiveDefault
	})

	return cmd
}

// getCommandFunc executes the "get" command.
func getCommandFunc(cmd *cobra.Command, args []string) {
	key, opts := getGetOp(args)
	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Get(ctx, key, opts...)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	if getCountOnly {
		if _, fields := display.(*fieldsPrinter); !fields {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("--count-only is only for `--write-out=fields`"))
		}
	}

	if printValueOnly {
		dp, simple := (display).(*simplePrinter)
		if !simple {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("print-value-only is only for `--write-out=simple`"))
		}
		dp.valueOnly = true
	}
	display.Get(*resp)
}

func getGetOp(args []string) (string, []clientv3.OpOption) {
	if len(args) == 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("get command needs one argument as key and an optional argument as range_end"))
	}

	if getPrefix && getFromKey {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--prefix` and `--from-key` cannot be set at the same time, choose one"))
	}

	if getKeysOnly && getCountOnly {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--keys-only` and `--count-only` cannot be set at the same time, choose one"))
	}

	var opts []clientv3.OpOption
	if IsSerializable(getConsistency) {
		opts = append(opts, clientv3.WithSerializable())
	}

	key := args[0]
	if len(args) > 1 {
		if getPrefix || getFromKey {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("too many arguments, only accept one argument when `--prefix` or `--from-key` is set"))
		}
		opts = append(opts, clientv3.WithRange(args[1]))
	}

	opts = append(opts, clientv3.WithLimit(getLimit))
	if getRev > 0 {
		opts = append(opts, clientv3.WithRev(getRev))
	}

	sortByOrder := clientv3.SortNone
	sortOrder := strings.ToUpper(getSortOrder)
	switch {
	case sortOrder == "ASCEND":
		sortByOrder = clientv3.SortAscend
	case sortOrder == "DESCEND":
		sortByOrder = clientv3.SortDescend
	case sortOrder == "":
		// nothing
	default:
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, fmt.Errorf("bad sort order %v", getSortOrder))
	}

	sortByTarget := clientv3.SortByKey
	sortTarget := strings.ToUpper(getSortTarget)
	switch {
	case sortTarget == "CREATE":
		sortByTarget = clientv3.SortByCreateRevision
	case sortTarget == "KEY":
		sortByTarget = clientv3.SortByKey
	case sortTarget == "MODIFY":
		sortByTarget = clientv3.SortByModRevision
	case sortTarget == "VALUE":
		sortByTarget = clientv3.SortByValue
	case sortTarget == "VERSION":
		sortByTarget = clientv3.SortByVersion
	case sortTarget == "":
		// nothing
	default:
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, fmt.Errorf("bad sort target %v", getSortTarget))
	}

	opts = append(opts, clientv3.WithSort(sortByTarget, sortByOrder))

	if getPrefix {
		if len(key) == 0 {
			key = "\x00"
			opts = append(opts, clientv3.WithFromKey())
		} else {
			opts = append(opts, clientv3.WithPrefix())
		}
	}

	if getFromKey {
		if len(key) == 0 {
			key = "\x00"
		}
		opts = append(opts, clientv3.WithFromKey())
	}

	if getKeysOnly {
		opts = append(opts, clientv3.WithKeysOnly())
	}

	if getCountOnly {
		opts = append(opts, clientv3.WithCountOnly())
	}

	if getMinCreateRev > 0 {
		opts = append(opts, clientv3.WithMinCreateRev(getMinCreateRev))
	}

	if getMaxCreateRev > 0 {
		if getMinCreateRev > getMaxCreateRev {
			cobrautl.ExitWithError(cobrautl.ExitBadFeature,
				fmt.Errorf("getMinCreateRev(=%v) > getMaxCreateRev(=%v)", getMinCreateRev, getMaxCreateRev))
		}
		opts = append(opts, clientv3.WithMaxCreateRev(getMaxCreateRev))
	}

	if getMinModRev > 0 {
		opts = append(opts, clientv3.WithMinModRev(getMinModRev))
	}

	if getMaxModRev > 0 {
		if getMinModRev > getMaxModRev {
			cobrautl.ExitWithError(cobrautl.ExitBadFeature,
				fmt.Errorf("getMinModRev(=%v) > getMaxModRev(=%v)", getMinModRev, getMaxModRev))
		}
		opts = append(opts, clientv3.WithMaxModRev(getMaxModRev))
	}

	return key, opts
}
