package memory

// These helpers run under the store's write lock. A failed snapshot must
// leave readers and retries observing the last successfully published state.
func putLocked[T any](s *Store, values map[string]T, id string, next T) error {
	previous, exists := values[id]
	values[id] = next
	if err := s.persistLocked(); err != nil {
		if exists {
			values[id] = previous
		} else {
			delete(values, id)
		}
		return err
	}
	return nil
}

func replaceLocked[T any](s *Store, target *T, next T) error {
	previous := *target
	*target = next
	if err := s.persistLocked(); err != nil {
		*target = previous
		return err
	}
	return nil
}

func deleteLocked[T any](s *Store, values map[string]T, id string) error {
	previous, exists := values[id]
	if !exists {
		return nil
	}
	delete(values, id)
	if err := s.persistLocked(); err != nil {
		values[id] = previous
		return err
	}
	return nil
}

// A claim may reclaim several expired leases as well as grant one new lease.
func persistClaimsLocked[T any](s *Store, values, previous map[string]T) error {
	if len(previous) == 0 {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		for id, value := range previous {
			values[id] = value
		}
		return err
	}
	return nil
}
