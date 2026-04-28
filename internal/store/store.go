package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/crypto/bcrypt"
)

var db *sql.DB

var ErrDuplicateUsername = errors.New("username already exists")
var ErrRoomNotFound = errors.New("room not found")
var ErrRoomAccessDenied = errors.New("room access denied")

type User struct {
	ID          string    `json:"id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"displayName"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Room struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
}

func Init(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	var err error
	db, err = sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.PingContext(ctx); err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, `
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        display_name TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS rooms (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        created_by TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS room_members (
        room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        joined_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        PRIMARY KEY (room_id, user_id)
    );
    `)
	return err
}

func CreateUser(ctx context.Context, id, username, password, displayName string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(
		ctx,
		`INSERT INTO users (id, username, password_hash, display_name) VALUES ($1,$2,$3,$4)`,
		id,
		username,
		string(hash),
		displayName,
	)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
			return ErrDuplicateUsername
		}
		return err
	}
	return nil
}

func Authenticate(ctx context.Context, username, password string) (*User, error) {
	var u User
	var hash string
	row := db.QueryRowContext(
		ctx,
		`SELECT id, password_hash, display_name, created_at FROM users WHERE username=$1`,
		username,
	)

	if err := row.Scan(&u.ID, &hash, &u.DisplayName, &u.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return nil, nil
	}

	u.Username = username
	return &u, nil
}

func GetUserByID(ctx context.Context, id string) (*User, error) {
	var u User
	row := db.QueryRowContext(
		ctx,
		`SELECT id, username, display_name, created_at FROM users WHERE id=$1`,
		id,
	)
	if err := row.Scan(&u.ID, &u.Username, &u.DisplayName, &u.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

func CreateRoom(ctx context.Context, roomID, name, createdBy string) (*Room, error) {
	if name == "" {
		name = "Комната"
	}

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO rooms (id, name, created_by) VALUES ($1,$2,$3)`,
		roomID,
		name,
		createdBy,
	)
	if err != nil {
		return nil, err
	}

	if err := AddUserToRoom(ctx, roomID, createdBy); err != nil {
		return nil, err
	}

	return GetRoomByID(ctx, roomID)
}

func AddUserToRoom(ctx context.Context, roomID, userID string) error {
	if exists, err := RoomExists(ctx, roomID); err != nil {
		return err
	} else if !exists {
		return ErrRoomNotFound
	}

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO room_members (room_id, user_id) VALUES ($1,$2) ON CONFLICT (room_id, user_id) DO NOTHING`,
		roomID,
		userID,
	)
	return err
}

func IsUserInRoom(ctx context.Context, roomID, userID string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2)`,
		roomID,
		userID,
	).Scan(&exists)
	return exists, err
}

func RemoveUserFromRoom(ctx context.Context, roomID, userID string) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`,
		roomID,
		userID,
	)
	if err != nil {
		return err
	}

	var members int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM room_members WHERE room_id=$1`, roomID).Scan(&members); err != nil {
		return err
	}
	if members == 0 {
		_, err = db.ExecContext(ctx, `DELETE FROM rooms WHERE id=$1`, roomID)
	}
	return err
}

func ListRoomsForUser(ctx context.Context, userID string) ([]Room, error) {
	rows, err := db.QueryContext(ctx, `
        SELECT r.id, r.name, r.created_by, r.created_at
        FROM rooms r
        INNER JOIN room_members rm ON rm.room_id = r.id
        WHERE rm.user_id = $1
        ORDER BY r.created_at DESC
    `, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rooms := []Room{}
	for rows.Next() {
		var room Room
		if err := rows.Scan(&room.ID, &room.Name, &room.CreatedBy, &room.CreatedAt); err != nil {
			return nil, err
		}
		rooms = append(rooms, room)
	}
	return rooms, rows.Err()
}

func GetRoomByID(ctx context.Context, roomID string) (*Room, error) {
	var room Room
	err := db.QueryRowContext(
		ctx,
		`SELECT id, name, created_by, created_at FROM rooms WHERE id=$1`,
		roomID,
	).Scan(&room.ID, &room.Name, &room.CreatedBy, &room.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &room, nil
}

func RoomExists(ctx context.Context, roomID string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rooms WHERE id=$1)`, roomID).Scan(&exists)
	return exists, err
}
