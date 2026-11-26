package api

import (
	"dts/backend/internal/domain"
	"net/http"
)

func (s *Server) confirmCutoverFreeze(w http.ResponseWriter, r *http.Request) {
	s.confirmFreeze(w, r, "forward")
}
func (s *Server) confirmRollbackFreeze(w http.ResponseWriter, r *http.Request) {
	s.confirmFreeze(w, r, "reverse")
}
func (s *Server) confirmFreeze(w http.ResponseWriter, r *http.Request, direction string) {
	var req domain.WriteFreezeRequest
	if err := decode(r, &req); err != nil {
		apiError(w, 400, err)
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		req.IdempotencyKey = key
	}
	result, err := s.migrations.ConfirmWriteFreeze(r.Context(), r.PathValue("id"), direction, r.Header.Get("X-DTS-User"), req)
	if err != nil {
		apiError(w, 400, err)
		return
	}
	s.audit(r, "CONFIRM_WRITE_FREEZE", "migration", r.PathValue("id"), "direction="+direction+" operation="+result.ID)
	writeJSON(w, http.StatusOK, result)
}
func (s *Server) lifecycleOperations(w http.ResponseWriter, r *http.Request) {
	result, err := s.migrations.LifecycleOperations(r.Context(), r.PathValue("id"))
	if err != nil {
		apiError(w, 404, err)
		return
	}
	writeJSON(w, 200, result)
}
