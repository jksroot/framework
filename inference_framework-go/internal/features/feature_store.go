// Package features manages loading and retrieval of feature vectors for user IDs.
package features

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// Store manages feature vectors loaded from a JSON file, keyed by user_id.
type Store struct {
	features map[string]map[string]interface{}
}

// NewStore creates a Store and loads features from the given JSON file path.
func NewStore(featuresPath string) (*Store, error) {
	s := &Store{
		features: make(map[string]map[string]interface{}),
	}
	if err := s.loadFeatures(featuresPath); err != nil {
		// Non-fatal: log warning and return empty store
		log.Printf("WARNING: %v", err)
	}
	return s, nil
}

// loadFeatures reads and parses the features JSON file.
func (s *Store) loadFeatures(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("features file not found: %s: %w", path, err)
	}

	if err := json.Unmarshal(data, &s.features); err != nil {
		return fmt.Errorf("failed to parse features file: %w", err)
	}

	log.Printf("Loaded features for %d users.", len(s.features))
	return nil
}

// GetFeatureVector returns the feature vector for a given user_id, or nil if not found.
func (s *Store) GetFeatureVector(userID string) map[string]interface{} {
	return s.features[userID]
}

// GetFeaturesForUsers returns feature vectors for multiple user IDs.
// Missing users get an empty map.
func (s *Store) GetFeaturesForUsers(userIDs []string) map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{}, len(userIDs))
	for _, uid := range userIDs {
		feat, ok := s.features[uid]
		if ok {
			result[uid] = feat
		} else {
			result[uid] = make(map[string]interface{})
		}
	}
	return result
}
