package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var (
	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	db        *sql.DB
	jwtSecret = []byte("super-secret-key-change-in-production")
	clients   = make(map[*websocket.Conn]string)
	broadcast = make(chan Message)
	mu        sync.Mutex
)

type User struct {
	ID       int    `json:"id"`
	Nickname string `json:"nickname"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
	Email    string `json:"email,omitempty"`
	About    string `json:"about,omitempty"`
	Avatar   string `json:"avatar,omitempty"`
}

type Message struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Nickname string `json:"nickname"`
	Text     string `json:"text"`
	Time     string `json:"time"`
	ChatID   int    `json:"chat_id"`
	Avatar   string `json:"avatar,omitempty"`
}

type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname,omitempty"`
	Email    string `json:"email,omitempty"`
}

type AuthResponse struct {
	Success bool   `json:"success"`
	Token   string `json:"token,omitempty"`
	Error   string `json:"error,omitempty"`
	User    *User  `json:"user,omitempty"`
}

type SearchResponse struct {
	Success bool   `json:"success"`
	Users   []User `json:"users,omitempty"`
	Error   string `json:"error,omitempty"`
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" { log.Fatal("DATABASE_URL не установлен") }
	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil { log.Fatal(err) }
	defer db.Close()
	createTables()

	http.HandleFunc("/api/register", handleRegister)
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/search", handleSearch)
	http.HandleFunc("/api/messages", handleGetMessages)
	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/chat", serveChat)
	go handleMessages()

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Println("Сервер на порту " + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func createTables() {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, nickname TEXT NOT NULL, password TEXT NOT NULL, email TEXT DEFAULT '', about TEXT DEFAULT '', avatar TEXT DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, username TEXT NOT NULL, nickname TEXT NOT NULL, text TEXT NOT NULL, time TEXT NOT NULL, chat_id INTEGER DEFAULT 1, avatar TEXT DEFAULT '')`,
	} { db.Exec(q) }
}

func serveHome(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "index.html") }
func serveChat(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "chat.html") }

func handleRegister(w http.ResponseWriter, r *http.Request) {
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	if req.Username == "" || req.Password == "" || req.Nickname == "" { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Все поля обязательны"}); return }
	var existingID int
	if err := db.QueryRow("SELECT id FROM users WHERE username = $1", req.Username).Scan(&existingID); err == nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь уже существует"}); return }
	var userID int
	db.QueryRow("INSERT INTO users (username, nickname, password, email) VALUES ($1, $2, $3, $4) RETURNING id", req.Username, req.Nickname, req.Password, req.Email).Scan(&userID)
	token, _ := generateToken(req.Username, userID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &User{ID: userID, Nickname: req.Nickname, Username: req.Username}})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	var user User
	if err := db.QueryRow("SELECT id, username, nickname, email, about, avatar FROM users WHERE username = $1 AND password = $2", req.Username, req.Password).Scan(&user.ID, &user.Username, &user.Nickname, &user.Email, &user.About, &user.Avatar); err != nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"}); return }
	token, _ := generateToken(user.Username, user.ID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	rows, _ := db.Query("SELECT id, username, nickname, email, about, avatar FROM users WHERE username LIKE $1 OR nickname LIKE $2 LIMIT 20", "%"+q+"%", "%"+q+"%")
	defer rows.Close()
	var users []User
	for rows.Next() { var u User; rows.Scan(&u.ID, &u.Username, &u.Nickname, &u.Email, &u.About, &u.Avatar); users = append(users, u) }
	if users == nil { users = []User{} }
	json.NewEncoder(w).Encode(SearchResponse{Success: true, Users: users})
}

func handleGetMessages(w http.ResponseWriter, r *http.Request) {
	chatID := r.URL.Query().Get("chat_id")
	if chatID == "" { chatID = "1" }
	rows, _ := db.Query("SELECT id, username, nickname, text, time, avatar FROM messages WHERE chat_id = $1 ORDER BY id ASC LIMIT 100", chatID)
	defer rows.Close()
	var messages []Message
	for rows.Next() { var m Message; rows.Scan(&m.ID, &m.Username, &m.Nickname, &m.Text, &m.Time, &m.Avatar); messages = append(messages, m) }
	if messages == nil { messages = []Message{} }
	json.NewEncoder(w).Encode(messages)
}

func generateToken(username string, userID int) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"username": username, "user_id": userID, "exp": time.Now().Add(720 * time.Hour).Unix()}).SignedString(jwtSecret)
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	token, _ := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)
	nickname := getNickname(username)
	avatar := getAvatar(username)

	ws, _ := upgrader.Upgrade(w, r, nil)
	defer ws.Close()
	mu.Lock(); clients[ws] = username; mu.Unlock()
	broadcastOnlineCount()

	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil { mu.Lock(); delete(clients, ws); mu.Unlock(); broadcastOnlineCount(); break }
		msg.Username = username; msg.Nickname = nickname; msg.Avatar = avatar; msg.Time = time.Now().Format("15:04")
		if msg.ChatID == 0 { msg.ChatID = 1 }
		db.Exec("INSERT INTO messages (username, nickname, text, time, chat_id, avatar) VALUES ($1, $2, $3, $4, $5, $6)", msg.Username, msg.Nickname, msg.Text, msg.Time, msg.ChatID, avatar)
		broadcast <- msg
	}
}

func handleMessages() {
	for { msg := <-broadcast; mu.Lock(); for c := range clients { c.WriteJSON(map[string]interface{}{"username": msg.Username, "nickname": msg.Nickname, "text": msg.Text, "time": msg.Time, "chat_id": msg.ChatID, "avatar": "/uploads/"+msg.Avatar}) }; mu.Unlock() }
}

func broadcastOnlineCount() {
	mu.Lock(); count := len(clients); mu.Unlock()
	mu.Lock(); for c := range clients { c.WriteJSON(map[string]interface{}{"username": "system", "text": fmt.Sprintf("%d", count), "time": "online"}) }; mu.Unlock()
}

func getNickname(u string) string { var n string; db.QueryRow("SELECT nickname FROM users WHERE username = $1", u).Scan(&n); if n == "" { return u }; return n }
func getAvatar(u string) string { var a string; db.QueryRow("SELECT avatar FROM users WHERE username = $1", u).Scan(&a); return a }