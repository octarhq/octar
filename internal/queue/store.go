package queue

// QueueSpec is the persisted representation of a queue and its group configs.
// The Scheduler holds live *Queue instances; this spec is what gets saved to disk.
type QueueSpec struct {
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Groups    []GroupConfig `json:"groups"`
}

// Store defines the persistence boundary for queue declarations.
// Today: InMemoryStore. Tomorrow: DiskStore, SQLiteStore, etc.
type Store interface {
	SaveQueue(spec QueueSpec) error
	LoadQueues() ([]QueueSpec, error)
	DeleteQueue(namespace, name string) error
}

// InMemoryStore satisfies Store without any I/O.
// Queue declarations survive only for the lifetime of the process.
type InMemoryStore struct {
	specs map[string]QueueSpec // "namespace/name" → spec
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{specs: make(map[string]QueueSpec)}
}

func (s *InMemoryStore) SaveQueue(spec QueueSpec) error {
	s.specs[queueKey(spec.Namespace, spec.Name)] = spec
	return nil
}

func (s *InMemoryStore) LoadQueues() ([]QueueSpec, error) {
	out := make([]QueueSpec, 0, len(s.specs))
	for _, v := range s.specs {
		out = append(out, v)
	}
	return out, nil
}

func (s *InMemoryStore) DeleteQueue(namespace, name string) error {
	delete(s.specs, queueKey(namespace, name))
	return nil
}
