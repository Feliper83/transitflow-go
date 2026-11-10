// Package events defines the JSON message contracts shared across TransitFlow
// services. Field names here are the wire contract: every producer and consumer
// must agree on them. Kafka messages are JSON and keyed by the booking id.
package events

// Kafka topic names, shared so producers and consumers never drift.
const (
	TopicPaymentCommands = "payment.commands"
	TopicPaymentEvents   = "payment.events"
	TopicBookingEvents   = "booking.events"
)

// Payment outcome statuses carried in PaymentResultEvent.Status.
const (
	PaymentCompleted = "COMPLETED"
	PaymentFailed    = "FAILED"
)

// Terminal booking event types carried in BookingEvent.Type.
const (
	BookingConfirmed = "CONFIRMED"
	BookingCancelled = "CANCELLED"
)

// ProcessPaymentCommand is produced to payment.commands by the booking SAGA and
// consumed by payment-service. BookingID is the idempotency key.
type ProcessPaymentCommand struct {
	BookingID     int64   `json:"bookingId"`
	Amount        float64 `json:"amount"`
	CorrelationID string  `json:"correlationId"`
}

// PaymentResultEvent is produced to payment.events by payment-service. A single
// event type carries the outcome in Status (COMPLETED / FAILED) so consumers
// branch on a field, not on a message type — keeping producer and consumer
// decoupled.
type PaymentResultEvent struct {
	BookingID     int64   `json:"bookingId"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
	Reason        string  `json:"reason,omitempty"`
	CorrelationID string  `json:"correlationId"`
}

// IsCompleted reports whether the payment succeeded.
func (e PaymentResultEvent) IsCompleted() bool { return e.Status == PaymentCompleted }

// BookingEvent is produced to booking.events on a terminal booking state. Type
// is CONFIRMED or CANCELLED.
type BookingEvent struct {
	BookingID     int64  `json:"bookingId"`
	Type          string `json:"type"`
	PassengerName string `json:"passengerName"`
	Reason        string `json:"reason,omitempty"`
	CorrelationID string `json:"correlationId"`
}
