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

	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
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
	log.Println("Таблицы обновлены")
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "index.html")
}

func serveChat(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "chat.html")
}

func serveUploads(w http.ResponseWriter, r *http.Request) {
	http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))).ServeHTTP(w, r)
}

func getAvatarURL(avatar string) string {
	if avatar == "" {
		return ""
	}
	return "/uploads/" + avatar
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный формат данных"})
		return
	}
	if req.Username == "" || req.Password == "" || req.Nickname == "" {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Все поля обязательны"})
		return
	}
	if len(req.Username) < 3 {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Имя пользователя должно быть не менее 3 символов"})
		return
	}
	if len(req.Password) < 6 {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пароль должен быть не менее 6 символов"})
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
	json.NewEncoder(w).Encode(AuthResponse{
		Success: true, Token: token,
		User: &User{ID: userID, Nickname: req.Nickname, Username: req.Username, Email: req.Email},
	})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный формат данных"})
		return
	}

	var user User
	err := db.QueryRow("SELECT id, username, nickname, email, COALESCE(about,''), COALESCE(avatar,'') FROM users WHERE username = $1 AND password = $2",
		req.Username, req.Password).Scan(&user.ID, &user.Username, &user.Nickname, &user.Email, &user.About, &user.Avatar)
	if err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"})
		return
	}
	user.Avatar = getAvatarURL(user.Avatar)
	token, _ := generateToken(user.Username, user.ID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: "Пустой запрос"})
		return
	}
	rows, err := db.Query("SELECT id, username, nickname, COALESCE(email,''), COALESCE(about,''), COALESCE(avatar,'') FROM users WHERE username LIKE $1 OR nickname LIKE $2 LIMIT 20",
		"%"+query+"%", "%"+query+"%")
	if err != nil {
		json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: "Ошибка поиска"})
		return
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		rows.Scan(&u.ID, &u.Username, &u.Nickname, &u.Email, &u.About, &u.Avatar)
		u.Avatar = getAvatarURL(u.Avatar)
		users = append(users, u)
	}
	if users == nil {
		users = []User{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SearchResponse{Success: true, Users: users})
}

func handleGetMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chatID := r.URL.Query().Get("chat_id")
	if chatID == "" {
		chatID = "1"
	}
	rows, err := db.Query("SELECT id, username, nickname, text, time, COALESCE(avatar,'') FROM messages WHERE chat_id = $1 ORDER BY id ASC LIMIT 100", chatID)
	if err != nil {
		json.NewEncoder(w).Encode([]Message{})
		return
	}
	defer rows.Close()
	var messages []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Username, &m.Nickname, &m.Text, &m.Time, &m.Avatar); err != nil {
			continue
		}
		m.Avatar = getAvatarURL(m.Avatar)
		messages = append(messages, m)
	}
	if messages == nil {
		messages = []Message{}
	}
	json.NewEncoder(w).Encode(messages)
}

func handleProfile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// GET — открытый доступ
	if r.Method == "GET" {
		userParam := r.URL.Query().Get("username")
		if userParam == "" {
			json.NewEncoder(w).Encode(map[string]string{"error": "username required"})
			return
		}
		var user User
		err := db.QueryRow("SELECT id, username, nickname, COALESCE(email,''), COALESCE(about,''), COALESCE(avatar,'') FROM users WHERE username = $1", userParam).
			Scan(&user.ID, &user.Username, &user.Nickname, &user.Email, &user.About, &user.Avatar)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}
		user.Avatar = getAvatarURL(user.Avatar)
		json.NewEncoder(w).Encode(user)
		return
	}

	// PUT — нужна авторизация
	if r.Method == "PUT" {
		tokenString := r.Header.Get("Authorization")
		if tokenString == "" || !strings.HasPrefix(tokenString, "Bearer ") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		tokenString = strings.TrimPrefix(tokenString, "Bearer ")
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
		if err != nil || !token.Valid {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}
		claims := token.Claims.(jwt.MapClaims)
		currentUser := claims["username"].(string)

		var req struct {
			Nickname string `json:"nickname,omitempty"`
			About    string `json:"about,omitempty"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.Nickname != "" {
			db.Exec("UPDATE users SET nickname = $1 WHERE username = $2", req.Nickname, currentUser)
		}
		if req.About != "" {
			db.Exec("UPDATE users SET about = $1 WHERE username = $2", req.About, currentUser)
		}
		json.NewEncoder(w).Encode(map[string]bool{"success": true})
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func handleUploadAvatar(w http.ResponseWriter, r *http.Request) {
	tokenString := r.Header.Get("Authorization")
	if tokenString == "" || !strings.HasPrefix(tokenString, "Bearer ") {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	tokenString = strings.TrimPrefix(tokenString, "Bearer ")
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	if err != nil || !token.Valid {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)

	file, header, err := r.FormFile("avatar")
	if err != nil {
		http.Error(w, "Ошибка загрузки", http.StatusBadRequest)
		return
	}
	defer file.Close()

	ext := filepath.Ext(header.Filename)
	filename := username + "_" + strconv.FormatInt(time.Now().Unix(), 10) + ext
	out, err := os.Create("uploads/" + filename)
	if err != nil {
		http.Error(w, "Ошибка сохранения", http.StatusInternalServerError)
		return
	}
	defer out.Close()
	io.Copy(out, file)

	db.Exec("UPDATE users SET avatar = $1 WHERE username = $2", filename, username)
	json.NewEncoder(w).Encode(map[string]string{"avatar": "/uploads/" + filename})
}

func generateToken(username string, userID int) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username,
		"user_id":  userID,
		"exp":      time.Now().Add(720 * time.Hour).Unix(),
	})
	return token.SignedString(jwtSecret)
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	if err != nil || !token.Valid {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)
	nickname := getNickname(username)
	avatar := getAvatar(username)

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	mu.Lock()
	clients[ws] = username
	mu.Unlock()

	log.Printf("%s подключился. Всего клиентов: %d", username, len(clients))
	broadcastOnlineCount()

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			mu.Lock()
			delete(clients, ws)
			mu.Unlock()
			broadcastOnlineCount()
			break
		}
		msg.Username = username
		msg.Nickname = nickname
		msg.Avatar = getAvatarURL(avatar)
		msg.Time = time.Now().Format("15:04")
		if msg.ChatID == 0 {
			msg.ChatID = 1
		}
		db.Exec("INSERT INTO messages (username, nickname, text, time, chat_id, avatar) VALUES ($1, $2, $3, $4, $5, $6)",
			msg.Username, msg.Nickname, msg.Text, msg.Time, msg.ChatID, avatar)
		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		mu.Lock()
		for client := range clients {
			client.WriteJSON(map[string]interface{}{
				"username": msg.Username,
				"nickname": msg.Nickname,
				"text":     msg.Text,
				"time":     msg.Time,
				"chat_id":  msg.ChatID,
				"avatar":   msg.Avatar,
			})
		}
		mu.Unlock()
	}
}

func broadcastOnlineCount() {
	mu.Lock()
	count := len(clients)
	mu.Unlock()
	mu.Lock()
	for client := range clients {
		client.WriteJSON(map[string]interface{}{
			"username": "system",
			"text":     fmt.Sprintf("%d", count),
			"time":     "online",
		})
	}
	mu.Unlock()
}

func getNickname(username string) string {
	var n string
	db.QueryRow("SELECT nickname FROM users WHERE username = $1", username).Scan(&n)
	if n == "" {
		return username
	}
	return n
}

func getAvatar(username string) string {
	var a string
	db.QueryRow("SELECT COALESCE(avatar,'') FROM users WHERE username = $1", username).Scan(&a)
	return a
}