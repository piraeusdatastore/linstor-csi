package volume

import (
	"reflect"
	"sort"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
)

// RemoteAccessPolicy represents a policy for allowing diskless access.
type RemoteAccessPolicy []RemoteAccessPolicyRule

type RemoteAccessPolicyRule struct {
	FromSame []string `yaml:"fromSame,omitempty"`
}

// AccessibleSegments applies the policy to a specific cluster segment.
func (r RemoteAccessPolicy) AccessibleSegments(segments map[string]string) []map[string]string {
	var result []map[string]string

	for _, rule := range r {
		m := make(map[string]string)

		for _, propName := range rule.FromSame {
			val, ok := segments[propName]
			if !ok {
				continue
			}

			m[propName] = val
		}

		result = append(result, m)
	}

	return PrunePattern(result...)
}

func (r *RemoteAccessPolicy) UnmarshalText(text []byte) error {
	// Handle legacy values: "true" allows access from any node, "false" allows access only on replicas
	b, err := strconv.ParseBool(string(text))
	if err == nil {
		if b {
			*r = RemoteAccessPolicyAnywhere
		} else {
			*r = RemoteAccessPolicyLocalOnly
		}

		return nil
	}

	// Cast is needed for reflect to pick up on the fact it's dealing with a pointer to a slice.
	return yaml.Unmarshal(text, (*[]RemoteAccessPolicyRule)(r))
}

func (r RemoteAccessPolicy) MarshalText() (text []byte, err error) {
	if reflect.DeepEqual(r, RemoteAccessPolicyLocalOnly) {
		return []byte("false"), nil
	} else if reflect.DeepEqual(r, RemoteAccessPolicyAnywhere) {
		return []byte("true"), nil
	}

	return yaml.Marshal([]RemoteAccessPolicyRule(r))
}

// String is used for settings a RemoteAccessPolicy via cli args.
func (r *RemoteAccessPolicy) String() string {
	text, err := r.MarshalText()
	if err != nil {
		panic(err)
	}

	return string(text)
}

// Set is used for setting a RemoteAccessPolicy via cli args.
func (r *RemoteAccessPolicy) Set(s string) error {
	return r.UnmarshalText([]byte(s))
}

var (
	// RemoteAccessPolicyAnywhere allows remote access from anywhere
	RemoteAccessPolicyAnywhere = RemoteAccessPolicy{{}}
	// RemoteAccessPolicyLocalOnly allows access only to the local node, effectively disabling diskless access.
	RemoteAccessPolicyLocalOnly = RemoteAccessPolicy{{FromSame: []string{topology.LinstorNodeKey}}}
)

// subsetOf returns true if the first argument is a subset of the second argument.
func subsetOf(a, b map[string]string) bool {
	for k, v := range a {
		other, ok := b[k]
		if !ok || v != other {
			return false
		}
	}

	return true
}

// PrunePattern returns the given list of pattern, removed of any duplicates or patterns which are already covered by
// a more general pattern.
//
// Some examples
//
//	[{a:1}, {a:1}] => [{a:1}]
//	[{a:1}, {a:2}] => [{a:1}, {a:2}]
//	[{a:1, b:1}, {a:1}] => [{a:1}]
//	[{a:1, b:1}, {a:1, b:2}] => [{a:1, b:1}, {a:1, b:2}]
//	[{a:1}, {a:1, b:1}, {a:1, b:2}] => [{a:1}]
func PrunePattern(sources ...map[string]string) []map[string]string {
	var result []map[string]string

	// This sort ensures that broad segments, i.e. those with less "rules", are inserted into the result first.
	// The reason you want that is you can avoid situations were a policy you are currently inspecting
	// would replace multiple existing ones. Basically, you don't want a situation where:
	//   toCheck ⊂ existing1 and toCheck ⊂ existing2
	// which would mean we would remove multiple existing elements and replace them with toCheck. By sorting we can
	// ensure that toCheck ⊂ existing1 => toCheck == existing1, so we can just skip insertion. This makes the actual
	// prune loop much easier.
	sort.Slice(sources, func(i, j int) bool {
		return len(sources[i]) < len(sources[j])
	})

outer:
	for _, source := range sources {
		for i := range result {
			if subsetOf(result[i], source) {
				// we already have the less strict variant in the results, no need to add a stricter variant
				continue outer
			}
		}

		result = append(result, source)
	}

	return result
}
