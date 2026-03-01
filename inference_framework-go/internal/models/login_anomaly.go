package models

import (
	"inference_framework/internal/config"
	"inference_framework/internal/output"
	"log"
	"math"
	"math/rand"
)

// LoginAnomaly detects login anomalies based on user features.
type LoginAnomaly struct {
	BaseModel
	Threshold float64
}

// NewLoginAnomaly creates a new LoginAnomaly model with the given config.
func NewLoginAnomaly(cfg config.ModelConfig) *LoginAnomaly {
	threshold := cfg.Threshold
	if threshold == 0 {
		threshold = 0.5
	}
	return &LoginAnomaly{
		BaseModel: NewBaseModel(cfg, "loginanomaly"),
		Threshold: threshold,
	}
}

// Process implements the Model interface for login anomaly detection.
// Assigns a random score (0-1) to each event as specified.
func (la *LoginAnomaly) Process(messages []map[string]interface{}, features map[string]map[string]interface{}) []output.Event {
	var events []output.Event

	for _, row := range messages {
		userID, _ := row["user_id"].(string)
		if userID == "" {
			continue
		}
		feat, ok := features[userID]
		if !ok || len(feat) == 0 {
			continue
		}

		// Simple scoring logic (base)
		ipRisk := getNestedFloat(feat, "ip_address_profile", "risk_score")
		travelFreq := getNestedFloat(feat, "country_code_profile", "travel_freq")
		baseScore := math.Min(1.0, (ipRisk+travelFreq*0.1)*1.5)

		// Assign random score (0-1) to event as per requirements
		score := rand.Float64()

		reason := "unusual_country"
		if ipRisk > 0.5 {
			reason = "high_risk_ip"
		}

		data := map[string]interface{}{
			"ip_address":   row["ip_address"],
			"country_code": row["country_code"],
			"user_agent":   row["user_agent"],
			"reason":       reason,
			"base_score":   baseScore,
		}

		// Filter on random score against threshold
		if score >= la.Threshold {
			event := GenerateOutputEvent(userID, score, data, la.ModelID)
			events = append(events, event)
			log.Printf("Detected anomaly for user %s with random score %f", userID, score)
		}
	}

	return events
}

// getNestedFloat extracts a float64 from a nested map structure.
func getNestedFloat(m map[string]interface{}, keys ...string) float64 {
	current := m
	for i, key := range keys {
		val, ok := current[key]
		if !ok {
			return 0.0
		}
		if i == len(keys)-1 {
			// Last key — extract float
			switch v := val.(type) {
			case float64:
				return v
			case int:
				return float64(v)
			default:
				return 0.0
			}
		}
		// Intermediate key — must be a map
		next, ok := val.(map[string]interface{})
		if !ok {
			return 0.0
		}
		current = next
	}
	return 0.0
}
