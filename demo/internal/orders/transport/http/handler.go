package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/odestroyero/debezium-demo-lab/internal/orders/domain"
	"github.com/odestroyero/debezium-demo-lab/internal/orders/service"
)

type Handler struct {
	orders *service.Service
}

type completeCheckoutRequest struct {
	ShopID      string `json:"shop_id"`
	TotalAmount int64  `json:"total_amount"`
}

type completeCheckoutResponse struct {
	CheckoutID    string `json:"checkout_id"`
	OrderID       string `json:"order_id"`
	OutboxEventID string `json:"outbox_event_id"`
	Topic         string `json:"topic"`
}

func New(orders *service.Service) *Handler {
	return &Handler{orders: orders}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("POST /checkouts/{checkoutID}/complete", h.completeCheckout)
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) completeCheckout(w http.ResponseWriter, r *http.Request) {
	req := completeCheckoutRequest{
		ShopID:      "shop_77",
		TotalAmount: 2490,
	}
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid json body")
			return
		}
	}

	result, err := h.orders.CompleteCheckout(r.Context(), domain.CompleteCheckoutCommand{
		CheckoutID:  strings.TrimSpace(r.PathValue("checkoutID")),
		ShopID:      strings.TrimSpace(req.ShopID),
		TotalAmount: req.TotalAmount,
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalidCheckoutCommand):
			writeError(w, http.StatusBadRequest, "checkout_id, shop_id, and positive total_amount are required")
		case errors.Is(err, domain.ErrCheckoutAlreadyCompleted):
			writeError(w, http.StatusConflict, "checkout was already completed")
		default:
			log.Printf("complete checkout failed: %v", err)
			writeError(w, http.StatusInternalServerError, "complete checkout")
		}
		return
	}

	writeJSON(w, http.StatusCreated, completeCheckoutResponse{
		CheckoutID:    result.CheckoutID,
		OrderID:       result.OrderID,
		OutboxEventID: result.OutboxEventID,
		Topic:         result.Topic,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
