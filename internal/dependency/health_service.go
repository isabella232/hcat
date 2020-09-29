package dependency

import (
	"encoding/gob"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/hcat/dep"
	"github.com/pkg/errors"
)

const (
	HealthAny      = "any"
	HealthPassing  = "passing"
	HealthWarning  = "warning"
	HealthCritical = "critical"
	HealthMaint    = "maintenance"

	NodeMaint    = "_node_maintenance"
	ServiceMaint = "_service_maintenance:"
)

var (
	// Ensure implements
	_ isDependency = (*HealthServiceQuery)(nil)

	// HealthServiceQueryRe is the regular expression to use.
	HealthServiceQueryRe = regexp.MustCompile(`\A` + tagRe + serviceNameRe + dcRe + nearRe + filterRe + `\z`)
)

func init() {
	gob.Register([]*HealthService{})
}

type HCLStruct interface {
	HCL()
}

// HealthService is a service entry in Consul.
type HealthService struct {
	Node                string            `hcl:"node"`
	NodeID              string            `hcl:"node_id"`
	NodeAddress         string            `hcl:"node_address"`
	NodeDatacenter      string            `hcl:"node_datacenter"`
	NodeTaggedAddresses map[string]string `hcl:"node_tagged_addresses"`
	NodeMeta            map[string]string `hcl:"node_meta"`
	ServiceMeta         map[string]string `hcl:"service_meta"`
	Address             string            `hcl:"address"`
	ID                  string            `hcl:"id"`
	Name                string            `hcl:"name"`
	Tags                ServiceTags       `hcl:"tags"`
	Checks              []HealthCheck     `hcl:"check,block"`
	Status              string            `hcl:"status"`
	Port                int               `hcl:"port"`
	Weights             api.AgentWeights
	Namespace           string `hcl:"namespace"`
}

func (h HealthService) HCL() {}

type HealthCheck struct {
	// Adding tags to api.HealthCheck could be done in consul
	Node        string   `hcl:"node"`
	CheckID     string   `hcl:"check_id"`
	Name        string   `hcl:"name"`
	Status      string   `hcl:"status"`
	Notes       string   `hcl:"notes"`
	Output      string   `hcl:"output"`
	ServiceID   string   `hcl:"service_id"`
	ServiceName string   `hcl:"service_name"`
	ServiceTags []string `hcl:"service_tags"`
	Type        string   `hcl:"type"`
	Namespace   string   `json:",omitempty",hcl:"namespace"`

	Definition api.HealthCheckDefinition // TODO tag this too
}

func (h HealthCheck) HCL() {}

// HealthServiceQuery is the representation of all a service query in Consul.
type HealthServiceQuery struct {
	isConsul
	stopCh chan struct{}

	dc      string
	filters []string
	name    string
	near    string
	tag     string
	connect bool
	opts    QueryOptions
}

// NewHealthServiceQuery processes the strings to build a service dependency.
func NewHealthServiceQuery(s string) (*HealthServiceQuery, error) {
	return healthServiceQuery(s, false)
}

// NewHealthConnect Query processes the strings to build a connect dependency.
func NewHealthConnectQuery(s string) (*HealthServiceQuery, error) {
	return healthServiceQuery(s, true)
}

func healthServiceQuery(s string, connect bool) (*HealthServiceQuery, error) {
	if !HealthServiceQueryRe.MatchString(s) {
		return nil, fmt.Errorf("health.service: invalid format: %q", s)
	}

	m := regexpMatch(HealthServiceQueryRe, s)

	var filters []string
	if filter := m["filter"]; filter != "" {
		split := strings.Split(filter, ",")
		for _, f := range split {
			f = strings.TrimSpace(f)
			switch f {
			case HealthAny,
				HealthPassing,
				HealthWarning,
				HealthCritical,
				HealthMaint:
				filters = append(filters, f)
			case "":
			default:
				return nil, fmt.Errorf(
					"health.service: invalid filter: %q in %q", f, s)
			}
		}
		sort.Strings(filters)
	} else {
		filters = []string{HealthPassing}
	}

	return &HealthServiceQuery{
		stopCh:  make(chan struct{}, 1),
		dc:      m["dc"],
		filters: filters,
		name:    m["name"],
		near:    m["near"],
		tag:     m["tag"],
		connect: connect,
	}, nil
}

// Fetch queries the Consul API defined by the given client and returns a slice
// of HealthService objects.
func (d *HealthServiceQuery) Fetch(clients dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	select {
	case <-d.stopCh:
		return nil, nil, ErrStopped
	default:
	}

	opts := d.opts.Merge(&QueryOptions{
		Datacenter: d.dc,
		Near:       d.near,
	})

	u := &url.URL{
		Path:     "/v1/health/service/" + d.name,
		RawQuery: opts.String(),
	}
	if d.tag != "" {
		q := u.Query()
		q.Set("tag", d.tag)
		u.RawQuery = q.Encode()
	}
	//log.Printf("[TRACE] %s: GET %s", d, u)

	// Check if a user-supplied filter was given. If so, we may be querying for
	// more than healthy services, so we need to implement client-side
	// filtering.
	passingOnly := len(d.filters) == 1 && d.filters[0] == HealthPassing

	nodes := clients.Consul().Health().Service
	if d.connect {
		nodes = clients.Consul().Health().Connect
	}
	entries, qm, err := nodes(d.name, d.tag, passingOnly, opts.ToConsulOpts())
	if err != nil {
		return nil, nil, errors.Wrap(err, d.String())
	}

	//log.Printf("[TRACE] %s: returned %d results", d, len(entries))

	list := make([]*HealthService, 0, len(entries))
	for _, entry := range entries {
		// Get the status of this service from its checks.
		status := entry.Checks.AggregatedStatus()

		// If we are not checking only healthy services, filter out services
		// that do not match the given filter.
		if !acceptStatus(d.filters, status) {
			continue
		}

		// Get the address of the service, falling back to the address of the
		// node.
		address := entry.Service.Address
		if address == "" {
			address = entry.Node.Address
		}

		// custom mapping to pick up tags
		checks := make([]HealthCheck, len(entry.Checks))
		for i, c := range entry.Checks {
			checks[i] = HealthCheck{
				Node:        c.Node,
				CheckID:     c.CheckID,
				Name:        c.Name,
				Status:      c.Status,
				Notes:       c.Notes,
				Output:      c.Output,
				ServiceID:   c.ServiceID,
				ServiceName: c.ServiceName,
				ServiceTags: c.ServiceTags,
				Type:        c.Type,
				Namespace:   c.Namespace,
				Definition:  c.Definition,
			}
		}

		list = append(list, &HealthService{
			Node:                entry.Node.Node,
			NodeID:              entry.Node.ID,
			NodeAddress:         entry.Node.Address,
			NodeDatacenter:      entry.Node.Datacenter,
			NodeTaggedAddresses: entry.Node.TaggedAddresses,
			NodeMeta:            entry.Node.Meta,
			ServiceMeta:         entry.Service.Meta,
			Address:             address,
			ID:                  entry.Service.ID,
			Name:                entry.Service.Service,
			Tags: ServiceTags(
				deepCopyAndSortTags(entry.Service.Tags)),
			Status:    status,
			Checks:    checks,
			Port:      entry.Service.Port,
			Weights:   entry.Service.Weights,
			Namespace: entry.Service.Namespace,
		})
	}

	//log.Printf("[TRACE] %s: returned %d results after filtering", d, len(list))

	// Sort unless the user explicitly asked for nearness
	if d.near == "" {
		sort.Stable(ByNodeThenID(list))
	}

	rm := &dep.ResponseMetadata{
		LastIndex:   qm.LastIndex,
		LastContact: qm.LastContact,
	}

	return list, rm, nil
}

// CanShare returns a boolean if this dependency is shareable.
func (d *HealthServiceQuery) CanShare() bool {
	return true
}

// Stop halts the dependency's fetch function.
func (d *HealthServiceQuery) Stop() {
	close(d.stopCh)
}

// String returns the human-friendly version of this dependency.
func (d *HealthServiceQuery) String() string {
	name := d.name
	if d.tag != "" {
		name = d.tag + "." + name
	}
	if d.dc != "" {
		name = name + "@" + d.dc
	}
	if d.near != "" {
		name = name + "~" + d.near
	}
	if len(d.filters) > 0 {
		name = name + "|" + strings.Join(d.filters, ",")
	}
	return fmt.Sprintf("health.service(%s)", name)
}

func (d *HealthServiceQuery) SetOptions(opts QueryOptions) {
	d.opts = opts
}

// acceptStatus allows us to check if a slice of health checks pass this filter.
func acceptStatus(list []string, s string) bool {
	for _, status := range list {
		if status == s || status == HealthAny {
			return true
		}
	}
	return false
}

// ByNodeThenID is a sortable slice of Service
type ByNodeThenID []*HealthService

// Len, Swap, and Less are used to implement the sort.Sort interface.
func (s ByNodeThenID) Len() int      { return len(s) }
func (s ByNodeThenID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByNodeThenID) Less(i, j int) bool {
	if s[i].Node < s[j].Node {
		return true
	} else if s[i].Node == s[j].Node {
		return s[i].ID <= s[j].ID
	}
	return false
}
