package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

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
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не установлен")
	}

	log.Println("Подключаюсь к PostgreSQL...")

	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatal("Ошибка sql.Open:", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Ошибка db.Ping:", err)
	}
	log.Println("Подключено к PostgreSQL успешно!")

	os.MkdirAll("uploads", 0755)
	createTables()

	http.HandleFunc("/api/register", handleRegister)
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/search", handleSearch)
	http.HandleFunc("/api/messages", handleGetMessages)
	http.HandleFunc("/api/profile", handleProfile)
	http.HandleFunc("/api/upload-avatar", handleUploadAvatar)
	http.HandleFunc("/api/dbtest", handleDBTest)
	http.HandleFunc("/api/dbusers", handleDBUsers)
	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/uploads/", serveUploads)
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/chat", serveChat)

	go handleMessages()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("Сервер запущен на порту " + port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("Ошибка запуска сервера:", err)
	}
}

func createTables() {
	db.Exec("DROP TABLE IF EXISTS messages")
	db.Exec("DROP TABLE IF EXISTS users")

	queries := []string{
		`CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username TEXT UNIQUE NOT NULL,
			nickname TEXT NOT NULL,
			password TEXT NOT NULL,
			email TEXT DEFAULT '',
			about TEXT DEFAULT '',
			avatar TEXT DEFAULT ''
		)`,
		`CREATE TABLE messages (
			id SERIAL PRIMARY KEY,
			username TEXT NOT NULL,
			nickname TEXT NOT NULL,
			text TEXT NOT NULL,
			time TEXT NOT NULL,
			chat_id INTEGER DEFAULT 1,
			avatar TEXT DEFAULT ''
		)`,
	}

	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			log.Println("Ошибка создания таблицы:", err)
		}
	}
	log.Println("Таблицы пересозданы")
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" { http.NotFound(w, r); return }
	http.ServeFile(w, r, "index.html")
}

func serveChat(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "chat.html")
}

func serveUploads(w http.ResponseWriter, r *http.Request) {
	http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))).ServeHTTP(w, r)
}

func getAvatarURL(avatar string) string {
	if avatar == "" { return "" }
	return "/uploads/" + avatar
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest
	json.NewDecoder(r.Body).Decode(&req)
	if req.Username == "" || req.Password == "" || req.Nickname == "" {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Все поля обязательны"})
		return
	}

	var existingID int
	err := db.QueryRow("SELECT id FROM users WHERE username = $1", req.Username).Scan(&existingID)
	if err == nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь уже существует"})
		return
	}

	var userID int
	err = db.QueryRow("INSERT INTO users (username, nickname, password, email) VALUES ($1, $2, $3, $4) RETURNING id",
		req.Username, req.Nickname, req.Password, req.Email).Scan(&userID)
	if err != nil {
		log.Println("Ошибка регистрации:", err)
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Ошибка сохранения"})
		return
	}

	token, _ := generateToken(req.Username, userID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &User{ID: userID, Nickname: req.Nickname, Username: req.Username, Email: req.Email}})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest
	json.NewDecoder(r.Body).Decode(&req)

	var user User
	var email, about, avatar sql.NullString
	err := db.QueryRow("SELECT id, username, nickname, email, about, avatar FROM users WHERE username = $1 AND password = $2",
		req.Username, req.Password).Scan(&user.ID, &user.Username, &user.Nickname, &email, &about, &avatar)
	if err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"})
		return
	}
	user.Email = email.String
	user.About = about.String
	user.Avatar = getAvatarURL(avatar.String)
	token, _ := generateToken(user.Username, user.ID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	query := r.URL.Query().Get("q")
	if query == "" {
		json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: "Пустой запрос"})
		return
	}
	rows, err := db.Query("SELECT id, username, nickname, email, about, avatar FROM users WHERE username ILIKE $1 OR nickname ILIKE $2 LIMIT 20",
		"%"+query+"%", "%"+query+"%")
	if err != nil {
		log.Println("Ошибка поиска:", err)
		json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		var email, about, avatar sql.NullString
		rows.Scan(&u.ID, &u.Username, &u.Nickname, &email, &about, &avatar)
		u.Email = email.String; u.About = about.String; u.Avatar = getAvatarURL(avatar.String)
		users = append(users, u)
	}
	if users == nil { users = []User{} }
	json.NewEncoder(w).Encode(SearchResponse{Success: true, Users: users})
}

func handleGetMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chatID := r.URL.Query().Get("chat_id")
	if chatID == "" { chatID = "1" }
	rows, err := db.Query("SELECT id, username, nickname, text, time, avatar FROM messages WHERE chat_id = $1 ORDER BY id ASC LIMIT 100", chatID)
	if err != nil { json.NewEncoder(w).Encode([]Message{}); return }
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		var m Message
		var avatar sql.NullString
		if err := rows.Scan(&m.ID, &m.Username, &m.Nickname, &m.Text, &m.Time, &avatar); err != nil { continue }
		m.Avatar = getAvatarURL(avatar.String)
		messages = append(messages, m)
	}
	if messages == nil { messages = []Message{} }
	json.NewEncoder(w).Encode(messages)
}

func handleProfile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		userParam := r.URL.Query().Get("username")
		if userParam == "" {
			json.NewEncoder(w).Encode(map[string]string{"error": "username required"})
			return
		}
		var user User
		var email, about, avatar sql.NullString
		err := db.QueryRow("SELECT id, username, nickname, email, about, avatar FROM users WHERE username = $1", userParam).
			Scan(&user.ID, &user.Username, &user.Nickname, &email, &about, &avatar)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}
		user.Email = email.String; user.About = about.String; user.Avatar = getAvatarURL(avatar.String)
		json.NewEncoder(w).Encode(user)
		return
	}

	if r.Method == "PUT" {
		tokenString := r.Header.Get("Authorization")
		if tokenString == "" || !strings.HasPrefix(tokenString, "Bearer ") { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
		tokenString = strings.TrimPrefix(tokenString, "Bearer ")
		token, _ := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
		claims := token.Claims.(jwt.MapClaims)
		currentUser := claims["username"].(string)
		var req struct {
			Nickname string `json:"nickname,omitempty"`
			About    string `json:"about,omitempty"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.Nickname != "" { db.Exec("UPDATE users SET nickname = $1 WHERE username = $2", req.Nickname, currentUser) }
		if req.About != "" { db.Exec("UPDATE users SET about = $1 WHERE username = $2", req.About, currentUser) }
		json.NewEncoder(w).Encode(map[string]bool{"success": true})
		return
	}
}

func handleUploadAvatar(w http.ResponseWriter, r *http.Request) {
	tokenString := r.Header.Get("Authorization")
	if tokenString == "" || !strings.HasPrefix(tokenString, "Bearer ") { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	tokenString = strings.TrimPrefix(tokenString, "Bearer ")
	token, _ := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)

	file, header, err := r.FormFile("avatar")
	if err != nil { http.Error(w, "Ошибка загрузки", http.StatusBadRequest); return }
	defer file.Close()

	ext := filepath.Ext(header.Filename)
	filename := username + "_" + strconv.FormatInt(time.Now().Unix(), 10) + ext
	out, _ := os.Create("uploads/" + filename)
	defer out.Close()
	io.Copy(out, file)

	db.Exec("UPDATE users SET avatar = $1 WHERE username = $2", filename, username)
	json.NewEncoder(w).Encode(map[string]string{"avatar": "/uploads/" + filename})
}

func handleDBTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var userCount int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	var tableExists bool
	db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')").Scan(&tableExists)
	json.NewEncoder(w).Encode(map[string]interface{}{"users_table": tableExists, "user_count": userCount})
}

func handleDBUsers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	rows, err := db.Query("SELECT id, username, nickname FROM users LIMIT 10")
	if err != nil { json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); return }
	defer rows.Close()
	var users []map[string]interface{}
	for rows.Next() {
		var id int; var un, nn string
		rows.Scan(&id, &un, &nn)
		users = append(users, map[string]interface{}{"id": id, "username": un, "nickname": nn})
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"count": len(users), "users": users})
}

func generateToken(username string, userID int) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username, "user_id": userID,
		"exp": time.Now().Add(720 * time.Hour).Unix(),
	}).SignedString(jwtSecret)
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	token, _ := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)
	nickname := getNickname(username)
	avatar := getAvatar(username)

	ws, _ := upgrader.Upgrade(w, r, nil)
	defer ws.Close()

	mu.Lock(); clients[ws] = username; mu.Unlock()
	log.Printf("%s подключился. Всего: %d", username, len(clients))
	broadcastOnlineCount()

	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil { mu.Lock(); delete(clients, ws); mu.Unlock(); broadcastOnlineCount(); break }
		msg.Username = username; msg.Nickname = nickname; msg.Avatar = getAvatarURL(avatar); msg.Time = time.Now().Format("15:04")
		if msg.ChatID == 0 { msg.ChatID = 1 }
		db.Exec("INSERT INTO messages (username, nickname, text, time, chat_id, avatar) VALUES ($1, $2, $3, $4, $5, $6)", msg.Username, msg.Nickname, msg.Text, msg.Time, msg.ChatID, avatar)
		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		mu.Lock()
		for c := range clients {
			c.WriteJSON(map[string]interface{}{"username": msg.Username, "nickname": msg.Nickname, "text": msg.Text, "time": msg.Time, "chat_id": msg.ChatID, "avatar": msg.Avatar})
		}
		mu.Unlock()
	}
}

func broadcastOnlineCount() {
	mu.Lock(); c := len(clients); mu.Unlock()
	mu.Lock()
	for cl := range clients {
		cl.WriteJSON(map[string]interface{}{"username": "system", "text": fmt.Sprintf("%d", c), "time": "online"})
	}
	mu.Unlock()
}

func getNickname(u string) string {
	var n string
	db.QueryRow("SELECT nickname FROM users WHERE username = $1", u).Scan(&n)
	if n == "" { return u }
	return n
}

func getAvatar(u string) string {
	var a sql.NullString
	db.QueryRow("SELECT avatar FROM users WHERE username = $1", u).Scan(&a)
	return a.String
}