package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/time/rate"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		EnableCompression: true,
	}

	db           *sql.DB
	jwtSecret    []byte
	clients      = make(map[*websocket.Conn]string)
	onlineUsers  = make(map[string]bool)
	blockedUsers = make(map[string]map[string]bool)
	typingUsers  = make(map[int]map[string]time.Time)
	broadcast    = make(chan Message)
	mu           sync.Mutex

	visitors   = make(map[string]*rate.Limiter)
	visitorsMu sync.Mutex
)

type User struct {
	ID       int    `json:"id"`
	Nickname string `json:"nickname"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
	Email    string `json:"email,omitempty"`
	About    string `json:"about,omitempty"`
	Avatar   string `json:"avatar,omitempty"`
	Phone    string `json:"phone,omitempty"`
}

type Message struct {
	ID        int    `json:"id"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	Text      string `json:"text"`
	Time      string `json:"time"`
	ChatID    int    `json:"chat_id"`
	Avatar    string `json:"avatar,omitempty"`
	Type      string `json:"type,omitempty"`
	Peer      string `json:"peer,omitempty"`
	Read      bool   `json:"read"`
	Edited    bool   `json:"edited"`
	FileURL   string `json:"file_url,omitempty"`
	FileType  string `json:"file_type,omitempty"`
	Action    string `json:"action,omitempty"`
	ReplyTo   int    `json:"reply_to,omitempty"`
	ReplyText string `json:"reply_text,omitempty"`
	ReplyNick string `json:"reply_nick,omitempty"`
}

type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname,omitempty"`
	Email    string `json:"email,omitempty"`
	Phone    string `json:"phone,omitempty"`
}

type AuthResponse struct {
	Success bool   `json:"success"`
	Token   string `json:"token,omitempty"`
	Error   string `json:"error,omitempty"`
	User    *User  `json:"user,omitempty"`
}

type SearchResponse struct {
	Success bool        `json:"success"`
	Users   []User      `json:"users,omitempty"`
	Groups  []GroupInfo `json:"groups,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type GroupInfo struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Avatar    string `json:"avatar,omitempty"`
	CreatedBy string `json:"created_by"`
	IsGroup   bool   `json:"is_group"`
}

type StickerPack struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Public bool   `json:"public"`
}

type Sticker struct {
	ID     int    `json:"id"`
	PackID int    `json:"pack_id"`
	URL    string `json:"url"`
}

type Gif struct {
	ID    int    `json:"id"`
	URL   string `json:"url"`
	Owner string `json:"owner"`
}

func getVisitor(ip string) *rate.Limiter {
	visitorsMu.Lock()
	defer visitorsMu.Unlock()
	limiter, exists := visitors[ip]
	if !exists {
		limiter = rate.NewLimiter(30, 60)
		visitors[ip] = limiter
	}
	return limiter
}

func rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			ip = strings.Split(forwarded, ",")[0]
		}
		if !getVisitor(ip).Allow() {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func recoveryMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("PANIC: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}

func withMiddleware(h http.HandlerFunc) http.HandlerFunc {
	return recoveryMiddleware(rateLimitMiddleware(h))
}

func main() {
	jwtSecret = []byte(os.Getenv("JWT_SECRET"))
	if len(jwtSecret) == 0 {
		jwtSecret = []byte("super-secret-key-change-in-production")
		log.Println("⚠️ JWT_SECRET не установлен!")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" { log.Fatal("DATABASE_URL не установлен") }

	log.Println("Подключаюсь к PostgreSQL...")
	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil { log.Fatal(err) }
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err = db.Ping(); err != nil { log.Fatal(err) }
	log.Println("Подключено к PostgreSQL успешно!")

	os.MkdirAll("uploads", 0755)
	os.MkdirAll("uploads/photos", 0755)
	os.MkdirAll("uploads/videos", 0755)
	os.MkdirAll("uploads/audio", 0755)
	os.MkdirAll("uploads/stickers", 0755)
	os.MkdirAll("uploads/gifs", 0755)
	createTables()

	http.HandleFunc("/api/register", withMiddleware(handleRegister))
	http.HandleFunc("/api/login", withMiddleware(handleLogin))
	http.HandleFunc("/api/search", withMiddleware(handleSearch))
	http.HandleFunc("/api/messages", withMiddleware(handleMessagesAPI))
	http.HandleFunc("/api/profile", withMiddleware(handleProfile))
	http.HandleFunc("/api/upload-avatar", withMiddleware(handleUploadAvatar))
	http.HandleFunc("/api/upload-file", withMiddleware(handleUploadFile))
	http.HandleFunc("/api/chat/create", withMiddleware(handleCreateChat))
	http.HandleFunc("/api/chat/list", withMiddleware(handleChatList))
	http.HandleFunc("/api/chat/delete", withMiddleware(handleDeleteChat))
	http.HandleFunc("/api/block", withMiddleware(handleBlockUser))
	http.HandleFunc("/api/group/create", withMiddleware(handleCreateGroup))
	http.HandleFunc("/api/group/join", withMiddleware(handleJoinGroup))
	http.HandleFunc("/api/group/info", withMiddleware(handleGroupInfoAPI))
	http.HandleFunc("/api/group/update", withMiddleware(handleUpdateGroup))
	http.HandleFunc("/api/group/members", withMiddleware(handleGroupMembers))
	http.HandleFunc("/api/group/invites", withMiddleware(handleGroupInvites))
	http.HandleFunc("/api/group/leave", withMiddleware(handleGroupLeave))
	http.HandleFunc("/api/stickers/packs", withMiddleware(handleStickerPacks))
	http.HandleFunc("/api/stickers/upload", withMiddleware(handleStickerUpload))
	http.HandleFunc("/api/stickers/list", withMiddleware(handleStickerList))
	http.HandleFunc("/api/gifs/upload", withMiddleware(handleGifUpload))
	http.HandleFunc("/api/gifs/list", withMiddleware(handleGifList))
	http.HandleFunc("/api/read", withMiddleware(handleMarkRead))
	http.HandleFunc("/api/online", withMiddleware(handleOnlineStatus))
	http.HandleFunc("/api/health", handleHealth)
	http.HandleFunc("/api/dbtest", withMiddleware(handleDBTest))
	http.HandleFunc("/api/clear-users", handleClearUsers)
	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/uploads/", serveUploads)
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/chat", serveChat)

	go handleMessages()
	go clearTypingStatuses()

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }

	server := &http.Server{Addr: ":" + port}
	go func() {
		log.Println("Сервер запущен на порту " + port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Завершение сервера...")
	mu.Lock()
	for ws := range clients { ws.Close() }
	mu.Unlock()
	server.Close()
	db.Close()
	log.Println("Сервер остановлен")
}

func createTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, nickname TEXT NOT NULL, password TEXT NOT NULL, email TEXT DEFAULT '', about TEXT DEFAULT '', avatar TEXT DEFAULT '', phone TEXT DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, username TEXT NOT NULL, nickname TEXT NOT NULL, text TEXT DEFAULT '', time TEXT NOT NULL, chat_id INTEGER DEFAULT 1, avatar TEXT DEFAULT '', read BOOLEAN DEFAULT false, edited BOOLEAN DEFAULT false, file_url TEXT DEFAULT '', file_type TEXT DEFAULT '', reply_to INTEGER DEFAULT 0, reply_text TEXT DEFAULT '', reply_nick TEXT DEFAULT '')`,
		`CREATE TABLE IF NOT EXISTS chats (id SERIAL PRIMARY KEY, user1 TEXT NOT NULL, user2 TEXT NOT NULL, UNIQUE(user1, user2))`,
		`CREATE TABLE IF NOT EXISTS groups_chat (id SERIAL PRIMARY KEY, name TEXT NOT NULL, avatar TEXT DEFAULT '', description TEXT DEFAULT '', created_by TEXT NOT NULL, created_at TEXT DEFAULT '', invite_code TEXT UNIQUE NOT NULL, public BOOLEAN DEFAULT false)`,
		`CREATE TABLE IF NOT EXISTS group_members (group_id INTEGER NOT NULL, username TEXT NOT NULL, role TEXT DEFAULT 'member', UNIQUE(group_id, username))`,
		`CREATE TABLE IF NOT EXISTS sticker_packs (id SERIAL PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL, public BOOLEAN DEFAULT false)`,
		`CREATE TABLE IF NOT EXISTS stickers (id SERIAL PRIMARY KEY, pack_id INTEGER NOT NULL, url TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS gifs (id SERIAL PRIMARY KEY, url TEXT NOT NULL, owner TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS blocked (id SERIAL PRIMARY KEY, username TEXT NOT NULL, blocked_username TEXT NOT NULL, UNIQUE(username, blocked_username))`,
	}
	for _, q := range queries { db.Exec(q) }
	db.Exec("CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
	db.Exec("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS read BOOLEAN DEFAULT false")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT false")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_url TEXT DEFAULT ''")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_type TEXT DEFAULT ''")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to INTEGER DEFAULT 0")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_text TEXT DEFAULT ''")
	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_nick TEXT DEFAULT ''")
	db.Exec("ALTER TABLE groups_chat ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''")
	db.Exec("ALTER TABLE groups_chat ADD COLUMN IF NOT EXISTS public BOOLEAN DEFAULT false")
	log.Println("Таблицы проверены")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mu.Lock(); online := len(onlineUsers); mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "online": online, "time": time.Now().Format(time.RFC3339)})
}

func handleClearUsers(w http.ResponseWriter, r *http.Request) {
	db.Exec("DELETE FROM users")
	db.Exec("DELETE FROM messages")
	db.Exec("DELETE FROM chats")
	db.Exec("DELETE FROM group_members")
	db.Exec("DELETE FROM groups_chat")
	db.Exec("DELETE FROM blocked")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Все данные удалены"})
}

func serveHome(w http.ResponseWriter, r *http.Request) { if r.URL.Path != "/" { http.NotFound(w, r); return }; http.ServeFile(w, r, "index.html") }
func serveChat(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "chat.html") }
func serveUploads(w http.ResponseWriter, r *http.Request) { http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))).ServeHTTP(w, r) }
func getAvatarURL(a string) string { if a == "" { return "" }; return "/uploads/" + a }
func getNickname(u string) string { var n string; db.QueryRow("SELECT nickname FROM users WHERE username = $1", u).Scan(&n); if n == "" { return u }; return n }
func getAvatar(u string) string { var a sql.NullString; db.QueryRow("SELECT avatar FROM users WHERE username = $1", u).Scan(&a); return a.String }
func generateToken(username string, userID int) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"username": username, "user_id": userID, "exp": time.Now().Add(720 * time.Hour).Unix()}).SignedString(jwtSecret)
}
func getUserFromRequest(r *http.Request) string {
	tokenString := r.Header.Get("Authorization")
	if tokenString == "" || !strings.HasPrefix(tokenString, "Bearer ") { return "" }
	tokenString = strings.TrimPrefix(tokenString, "Bearer ")
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	if err != nil || !token.Valid { return "" }
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok { return "" }
	return claims["username"].(string)
}
func isValidUsername(u string) bool { if len(u) < 6 { return false }; m, _ := regexp.MatchString(`^[a-zA-Z0-9_]+$`, u); return m }
func escapeHTML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;"); s = strings.ReplaceAll(s, "<", "&lt;"); s = strings.ReplaceAll(s, ">", "&gt;"); s = strings.ReplaceAll(s, "\"", "&quot;"); s = strings.ReplaceAll(s, "'", "&#39;"); return s
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	req.Username = strings.TrimSpace(escapeHTML(req.Username)); req.Nickname = strings.TrimSpace(escapeHTML(req.Nickname))
	if req.Username == "" || req.Password == "" || req.Nickname == "" { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Все поля обязательны"}); return }
	if !isValidUsername(req.Username) { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Юзернейм: от 6 символов, буквы, цифры, _"}); return }
	if len(req.Password) < 8 { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пароль: минимум 8 символов"}); return }
	var existingID int
	if db.QueryRow("SELECT id FROM users WHERE username = $1", req.Username).Scan(&existingID) == nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь уже существует"}); return }
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Ошибка сервера"}); return }
	var userID int
	db.QueryRow("INSERT INTO users (username, nickname, password, email, phone) VALUES ($1, $2, $3, $4, $5) RETURNING id", req.Username, req.Nickname, string(hashedPassword), req.Email, req.Phone).Scan(&userID)
	token, _ := generateToken(req.Username, userID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &User{ID: userID, Nickname: req.Nickname, Username: req.Username, Email: req.Email}})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	req.Username = strings.TrimSpace(escapeHTML(req.Username))
	var user User; var email, about, avatar, phone, hashedPassword sql.NullString
	err := db.QueryRow("SELECT id, username, nickname, password, email, about, avatar, phone FROM users WHERE username = $1", req.Username).Scan(&user.ID, &user.Username, &user.Nickname, &hashedPassword, &email, &about, &avatar, &phone)
	if err != nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"}); return }
	// Пробуем bcrypt
	if bcrypt.CompareHashAndPassword([]byte(hashedPassword.String), []byte(req.Password)) == nil {
		user.Email = email.String; user.About = about.String; user.Avatar = getAvatarURL(avatar.String); user.Phone = phone.String
		token, _ := generateToken(user.Username, user.ID)
		json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user}); return
	}
	// Миграция старого пароля
	if hashedPassword.String == req.Password {
		newHash, _ := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		db.Exec("UPDATE users SET password = $1 WHERE id = $2", string(newHash), user.ID)
		user.Email = email.String; user.About = about.String; user.Avatar = getAvatarURL(avatar.String); user.Phone = phone.String
		token, _ := generateToken(user.Username, user.ID)
		json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user}); return
	}
	json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query().Get("q")
	if q == "" { json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: "Пустой запрос"}); return }
	q = strings.TrimSpace(escapeHTML(q))
	rows, _ := db.Query("SELECT id, username, nickname, email, about, avatar, COALESCE(phone,'') FROM users WHERE username ILIKE $1 OR nickname ILIKE $2 LIMIT 20", "%"+q+"%", "%"+q+"%")
	defer rows.Close()
	var users []User
	for rows.Next() { var u User; var email, about, avatar, phone sql.NullString; rows.Scan(&u.ID, &u.Username, &u.Nickname, &email, &about, &avatar, &phone); u.Email = email.String; u.About = about.String; u.Avatar = getAvatarURL(avatar.String); u.Phone = phone.String; users = append(users, u) }
	if users == nil { users = []User{} }
	grows, _ := db.Query("SELECT id, name, COALESCE(avatar,''), created_by FROM groups_chat WHERE public = true AND name ILIKE $1 LIMIT 10", "%"+q+"%")
	defer grows.Close()
	var groups []GroupInfo
	for grows.Next() { var g GroupInfo; grows.Scan(&g.ID, &g.Name, &g.Avatar, &g.CreatedBy); g.Avatar = getAvatarURL(g.Avatar); g.IsGroup = true; groups = append(groups, g) }
	if groups == nil { groups = []GroupInfo{} }
	json.NewEncoder(w).Encode(SearchResponse{Success: true, Users: users, Groups: groups})
}

func handleMessagesAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	if r.Method == "GET" {
		chatID := r.URL.Query().Get("chat_id"); if chatID == "" { chatID = "1" }
		before := r.URL.Query().Get("before")
		var rows *sql.Rows
		if before != "" { rows, _ = db.Query("SELECT id, username, nickname, text, time, avatar, COALESCE(read,false), COALESCE(edited,false), COALESCE(file_url,''), COALESCE(file_type,''), COALESCE(reply_to,0), COALESCE(reply_text,''), COALESCE(reply_nick,'') FROM messages WHERE chat_id = $1 AND id < $2 ORDER BY id DESC LIMIT 50", chatID, before)
		} else { rows, _ = db.Query("SELECT id, username, nickname, text, time, avatar, COALESCE(read,false), COALESCE(edited,false), COALESCE(file_url,''), COALESCE(file_type,''), COALESCE(reply_to,0), COALESCE(reply_text,''), COALESCE(reply_nick,'') FROM messages WHERE chat_id = $1 ORDER BY id DESC LIMIT 50", chatID) }
		defer rows.Close()
		var messages []Message
		for rows.Next() { var m Message; var avatar sql.NullString; rows.Scan(&m.ID, &m.Username, &m.Nickname, &m.Text, &m.Time, &avatar, &m.Read, &m.Edited, &m.FileURL, &m.FileType, &m.ReplyTo, &m.ReplyText, &m.ReplyNick); m.Avatar = getAvatarURL(avatar.String); messages = append(messages, m) }
		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 { messages[i], messages[j] = messages[j], messages[i] }
		if messages == nil { messages = []Message{} }
		json.NewEncoder(w).Encode(messages); return
	}
	if r.Method == "PUT" {
		var req struct { ID int `json:"id"`; Text string `json:"text"` }; json.NewDecoder(r.Body).Decode(&req)
		var owner string; db.QueryRow("SELECT username FROM messages WHERE id = $1", req.ID).Scan(&owner)
		if owner != currentUser { http.Error(w, "Forbidden", http.StatusForbidden); return }
		req.Text = escapeHTML(strings.TrimSpace(req.Text))
		db.Exec("UPDATE messages SET text = $1, edited = true WHERE id = $2", req.Text, req.ID)
		json.NewEncoder(w).Encode(map[string]bool{"success": true}); return
	}
	if r.Method == "DELETE" {
		var req struct { ID int `json:"id"` }; json.NewDecoder(r.Body).Decode(&req)
		var owner string; db.QueryRow("SELECT username FROM messages WHERE id = $1", req.ID).Scan(&owner)
		if owner != currentUser { http.Error(w, "Forbidden", http.StatusForbidden); return }
		db.Exec("DELETE FROM messages WHERE id = $1", req.ID)
		json.NewEncoder(w).Encode(map[string]bool{"success": true}); return
	}
}

func handleProfile(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleUploadAvatar(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleUploadFile(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleCreateChat(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleDeleteChat(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleBlockUser(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func sendChatNotification(fromUser, toUser string, chatID int) { /* без изменений */ }
func handleChatList(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleOnlineStatus(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleMarkRead(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleDBTest(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func generateInviteCode() string { b := make([]byte, 8); rand.Read(b); return hex.EncodeToString(b) }
func handleCreateGroup(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleJoinGroup(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGroupInfoAPI(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleUpdateGroup(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGroupMembers(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGroupInvites(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGroupLeave(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleStickerPacks(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleStickerUpload(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleStickerList(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGifUpload(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleGifList(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func handleConnections(w http.ResponseWriter, r *http.Request) { /* без изменений */ }
func sendChatNotificationIfNew(username string, chatID int, nickname string) { /* без изменений */ }
func clearTypingStatuses() { /* без изменений */ }
func handleMessages() { /* без изменений */ }
func broadcastOnlineCount() { /* без изменений */ }