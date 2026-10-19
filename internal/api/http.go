// Package api 提供 HTTP 介面，方便用 curl 現場 Demo。
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"saga-points-demo/internal/errs"
	"saga-points-demo/internal/inventory"
	"saga-points-demo/internal/points"
	"saga-points-demo/internal/saga"
)

type Server struct {
	Orch      *saga.Orchestrator
	Points    *points.Service
	Inventory *inventory.Service
	Chaos     map[string]*saga.ChaosStep // step 名稱 → 故障注入器
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /redeem", s.redeem)
	mux.HandleFunc("GET /sagas/{request_id}", s.getSaga)
	mux.HandleFunc("GET /sagas/{request_id}/ledger", s.getLedger)
	mux.HandleFunc("GET /sagas/{request_id}/probe", s.probe)
	mux.HandleFunc("GET /users/{user_id}/points", s.getPoints)
	mux.HandleFunc("GET /items/{item_id}/stock", s.getStock)
	mux.HandleFunc("POST /admin/chaos", s.setChaos)
	mux.HandleFunc("POST /admin/recover", s.recoverNow)
	return mux
}

type redeemResponse struct {
	Message string     `json:"message"`
	Saga    *saga.Saga `json:"saga"`
}

func (s *Server) redeem(w http.ResponseWriter, r *http.Request) {
	var req saga.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 格式錯誤"})
		return
	}
	sg, err := s.Orch.Start(r.Context(), req)
	if err != nil {
		code := http.StatusInternalServerError
		if errs.IsPermanent(err) {
			code = http.StatusBadRequest
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, redeemResponse{Message: userMessage(sg), Saga: sg})
}

func userMessage(sg *saga.Saga) string {
	switch sg.Status {
	case saga.StatusCompleted:
		return "兌換成功"
	case saga.StatusFailed:
		return "兌換失敗：" + sg.LastError
	case saga.StatusCompensated:
		return "兌換失敗，點數已退還"
	case saga.StatusCompensating:
		return "兌換失敗，點數退還處理中"
	case saga.StatusNeedsManual:
		return "兌換失敗，客服人員將協助處理"
	default:
		return "處理中，請稍後查詢"
	}
}

func (s *Server) getSaga(w http.ResponseWriter, r *http.Request) {
	sg, err := s.Orch.Store().Get(r.Context(), r.PathValue("request_id"))
	if errors.Is(err, saga.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, sg)
}

// getLedger 列出某次兌換相關的點數流水（扣點、退點、作廢），對帳用。
func (s *Server) getLedger(w http.ResponseWriter, r *http.Request) {
	entries, err := s.Points.ListBySaga(r.Context(), r.PathValue("request_id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []points.Entry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

// probe 即時到各服務流水查證這次兌換每一步的真實狀態。
func (s *Server) probe(w http.ResponseWriter, r *http.Request) {
	sg, err := s.Orch.Store().Get(r.Context(), r.PathValue("request_id"))
	if errors.Is(err, saga.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"saga_status": sg.Status, "steps": s.Orch.Probe(r.Context(), sg)})
}

func (s *Server) getPoints(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id 格式錯誤"})
		return
	}
	b, err := s.Points.Balance(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"user_id": id, "balance": b})
}

func (s *Server) getStock(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("item_id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "item_id 格式錯誤"})
		return
	}
	v, err := s.Inventory.Stock(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"item_id": id, "stock": v})
}

type chaosRequest struct {
	Step         string `json:"step"`          // deduct_points / deduct_stock / grant_item
	ExecFailures int64  `json:"exec_failures"` // 正向要失敗幾次，-1 = 永遠
	CompFailures int64  `json:"comp_failures"` // 補償要失敗幾次，-1 = 永遠
	FailAfter    bool   `json:"fail_after"`    // true = 下游其實成功，只是回應遺失
}

func (s *Server) setChaos(w http.ResponseWriter, r *http.Request) {
	var req chaosRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 格式錯誤"})
		return
	}
	c, ok := s.Chaos[req.Step]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "未知的 step"})
		return
	}
	c.SetExecFailures(req.ExecFailures)
	c.SetCompFailures(req.CompFailures)
	c.SetFailAfter(req.FailAfter)
	writeJSON(w, http.StatusOK, req)
}

func (s *Server) recoverNow(w http.ResponseWriter, r *http.Request) {
	n, err := s.Orch.RecoverOnce(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"processed": n})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
