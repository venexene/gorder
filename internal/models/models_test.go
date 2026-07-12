package models

import (
	"testing"

	"github.com/go-playground/validator/v10"
)

func TestLoadOrderFromFile(t *testing.T) {
	if _, err := LoadOrderFromFile("../../testdata/order1.json"); err != nil {
		t.Errorf("failed to load order from file: %v", err)
	}

	if _, err := LoadOrderFromFile("../../testdata/order_no.json"); err == nil {
		t.Errorf("expected error for non-existent file")
	}

	if _, err := LoadOrderFromFile("../testdata/order_false.json"); err == nil {
		t.Errorf("expected error for invalid JSON")
	}

}

func TestOrderValidation(t *testing.T) {
	val := validator.New()

	validOrder, err := LoadOrderFromFile("../../testdata/order1.json")
	if err != nil {
		t.Errorf("failed to load order from file: %v", err)
	}

	if err := val.Struct(validOrder); err != nil {
		t.Errorf("failed to validate valid order: %v", err)
	}

	invalidOrder := validOrder
	invalidOrder.OrderUID = "fake-uuid"

	if err := val.Struct(invalidOrder); err == nil {
		t.Error("failed to catch invalid order")
	}
}
