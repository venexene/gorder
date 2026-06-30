package models

import (
	"testing"

	"github.com/go-playground/validator/v10"
)

// TestLoadOrderFromFile tests loading valid, missing, and malformed JSON order files.
func TestLoadOrderFromFile(t *testing.T) {
	if _, err := LoadOrderFromFile("../../testdata/order1.json"); err != nil {
		t.Errorf("Failed to load order from file: %v", err)
	}

	if _, err := LoadOrderFromFile("../../testdata/order_no.json"); err == nil {
		t.Errorf("Expected error for non-existent file")
	}

	if _, err := LoadOrderFromFile("../testdata/order_false.json"); err == nil {
		t.Errorf("Expected error for invalid JSON")
	}

}

// TestOrderValidation verifies that the validator catches invalid orders and accepts valid ones.
func TestOrderValidation(t *testing.T) {
	val := validator.New()

	validOrder, err := LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("Failed to load order from file: %v", err)
	}

	if err := val.Struct(validOrder); err != nil {
		t.Errorf("Failed to validate valid order: %v", err)
	}

	invalidOrder := validOrder
	invalidOrder.OrderUID = "fake-uuid"

	if err := val.Struct(invalidOrder); err == nil {
		t.Error("Failed to catch invalid order")
	}
}
