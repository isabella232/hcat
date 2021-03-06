package dependency

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/hcat/dep"
)

////////////
// FakeDep is a fake dependency that does not actually speaks to a server.
type FakeDep struct {
	Name string
}

func (d *FakeDep) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	time.Sleep(time.Microsecond)
	data := d.Name
	rm := &dep.ResponseMetadata{LastIndex: 1}
	return data, rm, nil
}

func (d *FakeDep) CanShare() bool {
	return true
}

func (d *FakeDep) String() string {
	return fmt.Sprintf("test_dep(%s)", d.Name)
}

func (d *FakeDep) Stop()                        {}
func (d *FakeDep) SetOptions(opts QueryOptions) {}

////////////
// FakeListDep is a fake dependency that does not actually speaks to a server.
// Returns a list, to allow for multi-pass template tests
type FakeListDep struct {
	Name string
	Data []string
}

func (d *FakeListDep) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	time.Sleep(time.Microsecond)
	data := d.Data
	rm := &dep.ResponseMetadata{LastIndex: 1}
	return data, rm, nil
}

func (d *FakeListDep) CanShare() bool {
	return true
}

func (d *FakeListDep) String() string {
	return fmt.Sprintf("test_list_dep(%s)", d.Name)
}

func (d *FakeListDep) Stop()                        {}
func (d *FakeListDep) SetOptions(opts QueryOptions) {}

////////////
// FakeDepStale is a fake dependency that can be used to test what happens
// when stale data is permitted.
type FakeDepStale struct {
	Name string
}

// Fetch is used to implement the dependency interface.
func (d *FakeDepStale) Fetch(clients dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	time.Sleep(time.Microsecond)

	opts := &QueryOptions{}

	if opts.AllowStale {
		data := "this is some stale data"
		rm := &dep.ResponseMetadata{LastIndex: 1, LastContact: 50 * time.Millisecond}
		return data, rm, nil
	}

	data := "this is some fresh data"
	rm := &dep.ResponseMetadata{LastIndex: 1}
	return data, rm, nil
}

func (d *FakeDepStale) CanShare() bool {
	return true
}

func (d *FakeDepStale) String() string {
	return fmt.Sprintf("test_dep_stale(%s)", d.Name)
}

func (d *FakeDepStale) Stop()                        {}
func (d *FakeDepStale) SetOptions(opts QueryOptions) {}

////////////
// FakeDepFetchError is a fake dependency that returns an error while fetching.
type FakeDepFetchError struct {
	Name string
}

func (d *FakeDepFetchError) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	time.Sleep(time.Microsecond)
	return nil, nil, fmt.Errorf("failed to contact server")
}

func (d *FakeDepFetchError) CanShare() bool {
	return true
}

func (d *FakeDepFetchError) String() string {
	return fmt.Sprintf("test_dep_fetch_error(%s)", d.Name)
}

func (d *FakeDepFetchError) Stop()                        {}
func (d *FakeDepFetchError) SetOptions(opts QueryOptions) {}

////////////
var _ isDependency = (*FakeDepSameIndex)(nil)

type FakeDepSameIndex struct{}

func (d *FakeDepSameIndex) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	meta := &dep.ResponseMetadata{LastIndex: 100}
	return nil, meta, nil
}

func (d *FakeDepSameIndex) CanShare() bool {
	return true
}

func (d *FakeDepSameIndex) String() string {
	return "test_dep_same_index"
}

func (d *FakeDepSameIndex) Stop()                        {}
func (d *FakeDepSameIndex) SetOptions(opts QueryOptions) {}

////////////
// FakeDepRetry is a fake dependency that errors on the first fetch and
// succeeds on subsequent fetches.
type FakeDepRetry struct {
	sync.Mutex
	Name    string
	retried bool
}

func (d *FakeDepRetry) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	time.Sleep(time.Microsecond)

	d.Lock()
	defer d.Unlock()

	if d.retried {
		data := "this is some data"
		rm := &dep.ResponseMetadata{LastIndex: 1}
		return data, rm, nil
	}

	d.retried = true
	return nil, nil, fmt.Errorf("failed to contact server (try again)")
}

func (d *FakeDepRetry) CanShare() bool {
	return true
}

func (d *FakeDepRetry) String() string {
	return fmt.Sprintf("test_dep_retry(%s)", d.Name)
}

func (d *FakeDepRetry) Stop()                        {}
func (d *FakeDepRetry) SetOptions(opts QueryOptions) {}

// FakeDepBlockingQuery is a fake dependency that blocks on Fetch for a
// duration to resemble Consul blocking queries.
type FakeDepBlockingQuery struct {
	Name          string
	Data          interface{}
	BlockDuration time.Duration
	Ctx           context.Context
	stop          chan struct{}
}

func (d *FakeDepBlockingQuery) Fetch(dep.Clients) (interface{}, *dep.ResponseMetadata, error) {
	if d.stop == nil {
		d.stop = make(chan struct{})
	}

	select {
	case <-d.stop:
		return nil, nil, dep.ErrStopped
	case <-time.After(d.BlockDuration):
		return d.Data, &dep.ResponseMetadata{LastIndex: 1}, nil
	case <-d.Ctx.Done():
		return nil, nil, d.Ctx.Err()
	}
}

func (d *FakeDepBlockingQuery) CanShare() bool {
	return true
}

func (d *FakeDepBlockingQuery) String() string {
	return fmt.Sprintf("test_dep_blocking_query(%s)", d.Name)
}

func (d *FakeDepBlockingQuery) Stop() {
	if d.stop != nil {
		close(d.stop)
	}
}

func (d *FakeDepBlockingQuery) SetOptions(opts QueryOptions) {}
