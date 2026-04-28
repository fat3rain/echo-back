package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"voicechat/internal/auth"
	"voicechat/internal/store"
	"voicechat/internal/ws"
)

func main() {
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		log.Fatal("store init:", err)
	}

	auth.Init()

	r := mux.NewRouter()

	r.HandleFunc("/api/register", handleRegister).Methods("POST")
	r.HandleFunc("/api/login", handleLogin).Methods("POST")
	r.HandleFunc("/api/me", handleMe).Methods("GET")
	r.HandleFunc("/api/rooms", handleListRooms).Methods("GET")
	r.HandleFunc("/api/rooms", handleCreateRoom).Methods("POST")
	r.HandleFunc("/api/rooms/{id}", handleGetRoom).Methods("GET")
	r.HandleFunc("/api/rooms/{id}/join", handleJoinRoom).Methods("POST")
	r.HandleFunc("/api/rooms/{id}", handleDeleteRoomForUser).Methods("DELETE")
	r.HandleFunc("/ws", ws.HandleWebSocket)
	r.PathPrefix("/").Handler(http.FileServer(http.Dir("static")))

	addr := ":8080"
	log.Printf("Starting server on %s\n", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}
	if req.Username == "" || req.Password == "" {
		http.Error(w, "empty", http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	if err := store.CreateUser(r.Context(), id, req.Username, req.Password, req.Username); err != nil {
		if errors.Is(err, store.ErrDuplicateUsername) {
			http.Error(w, "username already exists", http.StatusConflict)
			return
		}
		http.Error(w, "create user error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}

	u, err := store.Authenticate(r.Context(), req.Username, req.Password)
	if err != nil || u == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	tok, err := auth.GenerateToken(u.ID, u.Username, 24*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"token": tok})
}

func handleMe(w http.ResponseWriter, r *http.Request) {
	uid, err := requireUserID(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	u, err := store.GetUserByID(r.Context(), uid)
	if err != nil || u == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	_ = json.NewEncoder(w).Encode(u)
}

func handleListRooms(w http.ResponseWriter, r *http.Request) {
	uid, err := requireUserID(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	rooms, err := store.ListRoomsForUser(r.Context(), uid)
	if err != nil {
		http.Error(w, "rooms error", http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(rooms)
}

func handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	uid, err := requireUserID(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}

	roomID := strings.ToUpper(strings.ReplaceAll(uuid.New().String()[:8], "-", ""))
	room, err := store.CreateRoom(r.Context(), roomID, req.Name, uid)
	if err != nil {
		http.Error(w, "create room error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(room)
}

func handleGetRoom(w http.ResponseWriter, r *http.Request) {
	if _, err := requireUserID(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	roomID := mux.Vars(r)["id"]
	room, err := store.GetRoomByID(r.Context(), roomID)
	if err != nil {
		http.Error(w, "room error", http.StatusInternalServerError)
		return
	}
	if room == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	_ = json.NewEncoder(w).Encode(room)
}

func handleJoinRoom(w http.ResponseWriter, r *http.Request) {
	uid, err := requireUserID(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	roomID := mux.Vars(r)["id"]
	if err := store.AddUserToRoom(r.Context(), roomID, uid); err != nil {
		if errors.Is(err, store.ErrRoomNotFound) {
			http.Error(w, "room not found", http.StatusNotFound)
			return
		}
		http.Error(w, "join room error", http.StatusInternalServerError)
		return
	}

	room, err := store.GetRoomByID(r.Context(), roomID)
	if err != nil || room == nil {
		http.Error(w, "room error", http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(room)
}

func handleDeleteRoomForUser(w http.ResponseWriter, r *http.Request) {
	uid, err := requireUserID(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	roomID := mux.Vars(r)["id"]
	if err := store.RemoveUserFromRoom(r.Context(), roomID, uid); err != nil {
		http.Error(w, "delete room error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func requireUserID(r *http.Request) (string, error) {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return "", fmt.Errorf("missing auth")
	}

	var token string
	if n, _ := fmt.Sscanf(authz, "Bearer %s", &token); n != 1 {
		return "", fmt.Errorf("invalid auth")
	}

	uid, _, err := auth.ParseToken(token)
	if err != nil {
		return "", err
	}
	return uid, nil
}
