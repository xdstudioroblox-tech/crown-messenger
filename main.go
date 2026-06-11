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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var (
	upgrader     = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	db           *sql.DB
	jwtSecret    = []byte("super-secret-key-change-in-production")
	clients      = make(map[*websocket.Conn]string)
	onlineUsers  = make(map[string]bool)
	blockedUsers = make(map[string]map[string]bool)
	typingUsers  = make(map[int]map[string]time.Time)
	broadcast    = make(chan Message)
	mu           sync.Mutex
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

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" { log.Fatal("DATABASE_URL не установлен") }
	log.Println("Подключаюсь к PostgreSQL...")
	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil { log.Fatal(err) }
	defer db.Close()
	db.Ping()
	log.Println("Подключено к PostgreSQL успешно!")
	os.MkdirAll("uploads", 0755)
	os.MkdirAll("uploads/photos", 0755)
	os.MkdirAll("uploads/videos", 0755)
	os.MkdirAll("uploads/audio", 0755)
	os.MkdirAll("uploads/stickers", 0755)
	os.MkdirAll("uploads/gifs", 0755)
	createTables()

	http.HandleFunc("/api/register", handleRegister)
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/search", handleSearch)
	http.HandleFunc("/api/messages", handleMessagesAPI)
	http.HandleFunc("/api/profile", handleProfile)
	http.HandleFunc("/api/upload-avatar", handleUploadAvatar)
	http.HandleFunc("/api/upload-file", handleUploadFile)
	http.HandleFunc("/api/chat/create", handleCreateChat)
	http.HandleFunc("/api/chat/list", handleChatList)
	http.HandleFunc("/api/chat/delete", handleDeleteChat)
	http.HandleFunc("/api/block", handleBlockUser)
	http.HandleFunc("/api/clearchats", handleClearChats)
	http.HandleFunc("/api/dbtest", handleDBTest)
	http.HandleFunc("/api/online", handleOnlineStatus)
	http.HandleFunc("/api/read", handleMarkRead)
	http.HandleFunc("/api/group/create", handleCreateGroup)
	http.HandleFunc("/api/group/join", handleJoinGroup)
	http.HandleFunc("/api/group/info", handleGroupInfoAPI)
	http.HandleFunc("/api/group/update", handleUpdateGroup)
	http.HandleFunc("/api/group/members", handleGroupMembers)
	http.HandleFunc("/api/group/invites", handleGroupInvites)
	http.HandleFunc("/api/group/leave", handleGroupLeave)
	http.HandleFunc("/api/stickers/packs", handleStickerPacks)
	http.HandleFunc("/api/stickers/upload", handleStickerUpload)
	http.HandleFunc("/api/stickers/list", handleStickerList)
	http.HandleFunc("/api/gifs/upload", handleGifUpload)
	http.HandleFunc("/api/gifs/list", handleGifList)
	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/uploads/", serveUploads)
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/chat", serveChat)

	go handleMessages()
	go clearTypingStatuses()
	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Println("Сервер запущен на порту " + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
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
	db.Exec("DELETE FROM chats WHERE user1 = '' OR user2 = ''")
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

func serveHome(w http.ResponseWriter, r *http.Request) { if r.URL.Path != "/" { http.NotFound(w, r); return }; http.ServeFile(w, r, "index.html") }
func serveChat(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "chat.html") }
func serveUploads(w http.ResponseWriter, r *http.Request) { http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))).ServeHTTP(w, r) }
func getAvatarURL(a string) string { if a == "" { return "" }; return "/uploads/" + a }
func getNickname(u string) string { var n string; db.QueryRow("SELECT nickname FROM users WHERE username = $1", u).Scan(&n); if n == "" { return u }; return n }
func getAvatar(u string) string { var a sql.NullString; db.QueryRow("SELECT avatar FROM users WHERE username = $1", u).Scan(&a); return a.String }
func generateToken(username string, userID int) (string, error) { return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"username": username, "user_id": userID, "exp": time.Now().Add(720 * time.Hour).Unix()}).SignedString(jwtSecret) }
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

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	if req.Username == "" || req.Password == "" || req.Nickname == "" { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Все поля обязательны"}); return }
	if !isValidUsername(req.Username) { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Юзернейм: от 6 символов, только буквы, цифры и _"}); return }
	if len(req.Password) < 8 { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пароль: минимум 8 символов"}); return }
	var existingID int
	if db.QueryRow("SELECT id FROM users WHERE username = $1", req.Username).Scan(&existingID) == nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь уже существует"}); return }
	var userID int
	db.QueryRow("INSERT INTO users (username, nickname, password, email, phone) VALUES ($1, $2, $3, $4, $5) RETURNING id", req.Username, req.Nickname, req.Password, req.Email, req.Phone).Scan(&userID)
	token, _ := generateToken(req.Username, userID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &User{ID: userID, Nickname: req.Nickname, Username: req.Username, Email: req.Email}})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
	var req AuthRequest; json.NewDecoder(r.Body).Decode(&req)
	var user User; var email, about, avatar, phone sql.NullString
	if db.QueryRow("SELECT id, username, nickname, email, about, avatar, phone FROM users WHERE username = $1 AND password = $2", req.Username, req.Password).Scan(&user.ID, &user.Username, &user.Nickname, &email, &about, &avatar, &phone) != nil { json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный логин или пароль"}); return }
	user.Email = email.String; user.About = about.String; user.Avatar = getAvatarURL(avatar.String); user.Phone = phone.String
	token, _ := generateToken(user.Username, user.ID)
	json.NewEncoder(w).Encode(AuthResponse{Success: true, Token: token, User: &user})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query().Get("q")
	if q == "" { json.NewEncoder(w).Encode(SearchResponse{Success: false, Error: "Пустой запрос"}); return }
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
		rows, _ := db.Query("SELECT id, username, nickname, text, time, avatar, COALESCE(read,false), COALESCE(edited,false), COALESCE(file_url,''), COALESCE(file_type,''), COALESCE(reply_to,0), COALESCE(reply_text,''), COALESCE(reply_nick,'') FROM messages WHERE chat_id = $1 ORDER BY id ASC LIMIT 500", chatID)
		defer rows.Close()
		var messages []Message
		for rows.Next() { var m Message; var avatar sql.NullString; rows.Scan(&m.ID, &m.Username, &m.Nickname, &m.Text, &m.Time, &avatar, &m.Read, &m.Edited, &m.FileURL, &m.FileType, &m.ReplyTo, &m.ReplyText, &m.ReplyNick); m.Avatar = getAvatarURL(avatar.String); messages = append(messages, m) }
		if messages == nil { messages = []Message{} }
		json.NewEncoder(w).Encode(messages); return
	}
	if r.Method == "PUT" {
		var req struct { ID int `json:"id"`; Text string `json:"text"` }; json.NewDecoder(r.Body).Decode(&req)
		var owner string; db.QueryRow("SELECT username FROM messages WHERE id = $1", req.ID).Scan(&owner)
		if owner != currentUser { http.Error(w, "Forbidden", http.StatusForbidden); return }
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

func handleProfile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		userParam := r.URL.Query().Get("username")
		if userParam == "" { json.NewEncoder(w).Encode(map[string]string{"error": "username required"}); return }
		var user User; var email, about, avatar, phone sql.NullString
		if db.QueryRow("SELECT id, username, nickname, email, about, avatar, phone FROM users WHERE username = $1", userParam).Scan(&user.ID, &user.Username, &user.Nickname, &email, &about, &avatar, &phone) != nil { w.WriteHeader(http.StatusNotFound); return }
		user.Email = email.String; user.About = about.String; user.Avatar = getAvatarURL(avatar.String); user.Phone = phone.String
		json.NewEncoder(w).Encode(user); return
	}
	if r.Method == "PUT" {
		currentUser := getUserFromRequest(r)
		if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
		var req struct { Nickname string `json:"nickname,omitempty"`; About string `json:"about,omitempty"`; Phone string `json:"phone,omitempty"` }
		json.NewDecoder(r.Body).Decode(&req)
		if req.Nickname != "" { db.Exec("UPDATE users SET nickname = $1 WHERE username = $2", req.Nickname, currentUser) }
		if req.About != "" { db.Exec("UPDATE users SET about = $1 WHERE username = $2", req.About, currentUser) }
		if req.Phone != "" { db.Exec("UPDATE users SET phone = $1 WHERE username = $2", req.Phone, currentUser) }
		json.NewEncoder(w).Encode(map[string]bool{"success": true}); return
	}
}

func handleUploadAvatar(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	file, header, _ := r.FormFile("avatar")
	defer file.Close()
	ext := filepath.Ext(header.Filename)
	filename := "avatar_" + currentUser + "_" + strconv.FormatInt(time.Now().Unix(), 10) + ext
	out, _ := os.Create("uploads/" + filename)
	defer out.Close()
	io.Copy(out, file)
	db.Exec("UPDATE users SET avatar = $1 WHERE username = $2", filename, currentUser)
	json.NewEncoder(w).Encode(map[string]string{"avatar": "/uploads/" + filename})
}

func handleUploadFile(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	file, header, _ := r.FormFile("file")
	defer file.Close()
	ext := strings.ToLower(filepath.Ext(header.Filename))
	var folder, fileType string
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp": folder = "photos"; fileType = "photo"
	case ".mp4", ".webm", ".mov", ".avi": folder = "videos"; fileType = "video"
	case ".mp3", ".ogg", ".wav", ".m4a": folder = "audio"; fileType = "audio"
	default: folder = "photos"; fileType = "file"
	}
	filename := currentUser + "_" + strconv.FormatInt(time.Now().UnixNano(), 10) + ext
	out, _ := os.Create("uploads/" + folder + "/" + filename)
	defer out.Close()
	io.Copy(out, file)
	fileURL := "/uploads/" + folder + "/" + filename
	json.NewEncoder(w).Encode(map[string]string{"file_url": fileURL, "file_type": fileType})
}

func handleCreateChat(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { User2 string `json:"user2"` }; json.NewDecoder(r.Body).Decode(&req)
	if currentUser == req.User2 { http.Error(w, "Нельзя", http.StatusBadRequest); return }
	u1, u2 := currentUser, req.User2; if u1 > u2 { u1, u2 = u2, u1 }
	var chatID int
	err := db.QueryRow("SELECT id FROM chats WHERE user1 = $1 AND user2 = $2", u1, u2).Scan(&chatID)
	if err == nil { json.NewEncoder(w).Encode(map[string]int{"chat_id": chatID}); return }
	db.QueryRow("INSERT INTO chats (user1, user2) VALUES ($1, $2) RETURNING id", u1, u2).Scan(&chatID)
	json.NewEncoder(w).Encode(map[string]int{"chat_id": chatID})
}

func handleDeleteChat(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { ChatID int `json:"chat_id"` }; json.NewDecoder(r.Body).Decode(&req)
	db.Exec("DELETE FROM messages WHERE chat_id = $1", req.ChatID)
	db.Exec("DELETE FROM chats WHERE id = $1", req.ChatID)
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func handleBlockUser(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { Username string `json:"username"`; Block bool `json:"block"` }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Block {
		db.Exec("INSERT INTO blocked (username, blocked_username) VALUES ($1, $2) ON CONFLICT DO NOTHING", currentUser, req.Username)
		mu.Lock(); if blockedUsers[currentUser] == nil { blockedUsers[currentUser] = make(map[string]bool) }; blockedUsers[currentUser][req.Username] = true; mu.Unlock()
	} else {
		db.Exec("DELETE FROM blocked WHERE username = $1 AND blocked_username = $2", currentUser, req.Username)
		mu.Lock(); if blockedUsers[currentUser] != nil { delete(blockedUsers[currentUser], req.Username) }; mu.Unlock()
	}
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func sendChatNotification(fromUser, toUser string, chatID int) {
	mu.Lock(); defer mu.Unlock()
	if blockedUsers[toUser] != nil && blockedUsers[toUser][fromUser] { return }
	for ws, user := range clients { if user == toUser { ws.WriteJSON(map[string]interface{}{"type": "chat_created", "chat_id": chatID, "peer": fromUser, "username": "system"}) } }
}

func handleChatList(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var chats []map[string]interface{}
	chats = append(chats, map[string]interface{}{"chat_id": 1, "peer": "", "is_group": false})
	rows, _ := db.Query("SELECT c.id, c.user1, c.user2 FROM chats c WHERE (c.user1 = $1 OR c.user2 = $2) AND EXISTS (SELECT 1 FROM messages m WHERE m.chat_id = c.id)", currentUser, currentUser)
	defer rows.Close()
	for rows.Next() { var id int; var u1, u2 string; rows.Scan(&id, &u1, &u2); peer := u1; if peer == currentUser { peer = u2 }; chats = append(chats, map[string]interface{}{"chat_id": id, "peer": peer, "is_group": false}) }
	grows, _ := db.Query("SELECT g.id, g.name FROM groups_chat g JOIN group_members gm ON g.id = gm.group_id WHERE gm.username = $1", currentUser)
	defer grows.Close()
	for grows.Next() { var gid int; var gn string; grows.Scan(&gid, &gn); chats = append(chats, map[string]interface{}{"chat_id": gid, "peer": gn, "is_group": true}) }
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chats)
}

func handleClearChats(w http.ResponseWriter, r *http.Request) { db.Exec("DELETE FROM chats"); json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) }
func handleOnlineStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	un := r.URL.Query().Get("username")
	if un == "" { json.NewEncoder(w).Encode(map[string]bool{"online": false}); return }
	mu.Lock(); _, online := onlineUsers[un]; mu.Unlock()
	json.NewEncoder(w).Encode(map[string]bool{"online": online})
}
func handleMarkRead(w http.ResponseWriter, r *http.Request) {
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { ChatID int `json:"chat_id"` }; json.NewDecoder(r.Body).Decode(&req)
	db.Exec("UPDATE messages SET read = true WHERE chat_id = $1 AND username != $2 AND read = false", req.ChatID, currentUser)
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}
func handleDBTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var userCount int; db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	var tableExists bool; db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')").Scan(&tableExists)
	json.NewEncoder(w).Encode(map[string]interface{}{"users_table": tableExists, "user_count": userCount})
}

func generateInviteCode() string { b := make([]byte, 8); rand.Read(b); return hex.EncodeToString(b) }
func handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { Name string `json:"name"`; Public bool `json:"public"` }; json.NewDecoder(r.Body).Decode(&req)
	if req.Name == "" { json.NewEncoder(w).Encode(map[string]string{"error": "Название обязательно"}); return }
	inviteCode := generateInviteCode()
	var groupID int
	err := db.QueryRow("INSERT INTO groups_chat (name, created_by, created_at, invite_code, public) VALUES ($1, $2, $3, $4, $5) RETURNING id", req.Name, currentUser, time.Now().Format("2006-01-02 15:04"), inviteCode, req.Public).Scan(&groupID)
	if err != nil { json.NewEncoder(w).Encode(map[string]string{"error": "Ошибка создания"}); return }
	db.Exec("INSERT INTO group_members (group_id, username, role) VALUES ($1, $2, 'admin')", groupID, currentUser)
	json.NewEncoder(w).Encode(map[string]interface{}{"group_id": groupID, "invite_code": inviteCode, "name": req.Name})
}
func handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { Code string `json:"code"` }; json.NewDecoder(r.Body).Decode(&req)
	var groupID int; var groupName string
	err := db.QueryRow("SELECT id, name FROM groups_chat WHERE invite_code = $1", req.Code).Scan(&groupID, &groupName)
	if err != nil { json.NewEncoder(w).Encode(map[string]string{"error": "Группа не найдена"}); return }
	var exists int
	db.QueryRow("SELECT COUNT(*) FROM group_members WHERE group_id = $1 AND username = $2", groupID, currentUser).Scan(&exists)
	if exists > 0 { json.NewEncoder(w).Encode(map[string]interface{}{"group_id": groupID, "name": groupName}); return }
	db.Exec("INSERT INTO group_members (group_id, username, role) VALUES ($1, $2, 'member')", groupID, currentUser)
	json.NewEncoder(w).Encode(map[string]interface{}{"group_id": groupID, "name": groupName})
}
func handleGroupInfoAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	groupID := r.URL.Query().Get("id")
	var id int; var name, avatar, createdBy, createdAt, code, desc string; var public bool
	db.QueryRow("SELECT id, name, COALESCE(avatar,''), created_by, created_at, invite_code, COALESCE(description,''), COALESCE(public,false) FROM groups_chat WHERE id = $1", groupID).Scan(&id, &name, &avatar, &createdBy, &createdAt, &code, &desc, &public)
	json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "name": name, "avatar": getAvatarURL(avatar), "created_by": createdBy, "created_at": createdAt, "invite_code": code, "description": desc, "public": public})
}
func handleUpdateGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { ID int `json:"id"`; Name string `json:"name,omitempty"`; Description string `json:"description,omitempty"`; Public bool `json:"public,omitempty"` }; json.NewDecoder(r.Body).Decode(&req)
	var role string; db.QueryRow("SELECT role FROM group_members WHERE group_id = $1 AND username = $2", req.ID, currentUser).Scan(&role)
	if role != "admin" { http.Error(w, "Forbidden", http.StatusForbidden); return }
	if req.Name != "" { db.Exec("UPDATE groups_chat SET name = $1 WHERE id = $2", req.Name, req.ID) }
	if req.Description != "" { db.Exec("UPDATE groups_chat SET description = $1 WHERE id = $2", req.Description, req.ID) }
	db.Exec("UPDATE groups_chat SET public = $1 WHERE id = $2", req.Public, req.ID)
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}
func handleGroupMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	groupID := r.URL.Query().Get("id")
	rows, _ := db.Query("SELECT gm.username, gm.role, u.nickname, COALESCE(u.avatar,'') FROM group_members gm JOIN users u ON gm.username = u.username WHERE gm.group_id = $1", groupID)
	defer rows.Close()
	var members []map[string]interface{}
	for rows.Next() { var un, role, nn, av string; rows.Scan(&un, &role, &nn, &av); members = append(members, map[string]interface{}{"username": un, "nickname": nn, "role": role, "avatar": getAvatarURL(av)}) }
	if members == nil { members = []map[string]interface{}{} }
	json.NewEncoder(w).Encode(members)
}
func handleGroupInvites(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	groupID := r.URL.Query().Get("id")
	var role string; db.QueryRow("SELECT role FROM group_members WHERE group_id = $1 AND username = $2", groupID, currentUser).Scan(&role)
	if role != "admin" { http.Error(w, "Forbidden", http.StatusForbidden); return }
	var code string; db.QueryRow("SELECT invite_code FROM groups_chat WHERE id = $1", groupID).Scan(&code)
	json.NewEncoder(w).Encode(map[string]string{"invite_code": code})
}
func handleGroupLeave(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	var req struct { GroupID int `json:"group_id"` }; json.NewDecoder(r.Body).Decode(&req)
	db.Exec("DELETE FROM group_members WHERE group_id = $1 AND username = $2", req.GroupID, currentUser)
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func handleStickerPacks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	if r.Method == "POST" {
		var req struct { Name string `json:"name"` }; json.NewDecoder(r.Body).Decode(&req)
		if req.Name == "" { json.NewEncoder(w).Encode(map[string]string{"error": "Название обязательно"}); return }
		var packID int
		db.QueryRow("INSERT INTO sticker_packs (name, owner) VALUES ($1, $2) RETURNING id", req.Name, currentUser).Scan(&packID)
		json.NewEncoder(w).Encode(map[string]interface{}{"pack_id": packID, "name": req.Name}); return
	}
	rows, _ := db.Query("SELECT id, name, owner FROM sticker_packs WHERE owner = $1 OR public = true", currentUser)
	defer rows.Close()
	var packs []StickerPack
	for rows.Next() { var p StickerPack; rows.Scan(&p.ID, &p.Name, &p.Owner); packs = append(packs, p) }
	if packs == nil { packs = []StickerPack{} }
	json.NewEncoder(w).Encode(packs)
}
func handleStickerUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	packIDStr := r.FormValue("pack_id"); packID, _ := strconv.Atoi(packIDStr)
	var owner string; db.QueryRow("SELECT owner FROM sticker_packs WHERE id = $1", packID).Scan(&owner)
	if owner != currentUser { http.Error(w, "Forbidden", http.StatusForbidden); return }
	file, header, _ := r.FormFile("sticker")
	defer file.Close()
	filename := "sticker_" + currentUser + "_" + strconv.FormatInt(time.Now().UnixNano(), 10) + filepath.Ext(header.Filename)
	out, _ := os.Create("uploads/stickers/" + filename)
	defer out.Close()
	io.Copy(out, file)
	url := "/uploads/stickers/" + filename
	db.Exec("INSERT INTO stickers (pack_id, url) VALUES ($1, $2)", packID, url)
	json.NewEncoder(w).Encode(map[string]string{"url": url})
}
func handleStickerList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	packID := r.URL.Query().Get("pack_id")
	rows, _ := db.Query("SELECT id, pack_id, url FROM stickers WHERE pack_id = $1", packID)
	defer rows.Close()
	var stickers []Sticker
	for rows.Next() { var s Sticker; rows.Scan(&s.ID, &s.PackID, &s.URL); stickers = append(stickers, s) }
	if stickers == nil { stickers = []Sticker{} }
	json.NewEncoder(w).Encode(stickers)
}
func handleGifUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currentUser := getUserFromRequest(r)
	if currentUser == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	file, header, _ := r.FormFile("gif")
	defer file.Close()
	filename := "gif_" + currentUser + "_" + strconv.FormatInt(time.Now().UnixNano(), 10) + filepath.Ext(header.Filename)
	out, _ := os.Create("uploads/gifs/" + filename)
	defer out.Close()
	io.Copy(out, file)
	url := "/uploads/gifs/" + filename
	db.Exec("INSERT INTO gifs (url, owner) VALUES ($1, $2)", url, currentUser)
	json.NewEncoder(w).Encode(map[string]string{"url": url})
}
func handleGifList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	rows, _ := db.Query("SELECT id, url, owner FROM gifs ORDER BY id DESC LIMIT 50")
	defer rows.Close()
	var gifs []Gif
	for rows.Next() { var g Gif; rows.Scan(&g.ID, &g.URL, &g.Owner); gifs = append(gifs, g) }
	if gifs == nil { gifs = []Gif{} }
	json.NewEncoder(w).Encode(gifs)
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }
	token, _ := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) { return jwtSecret, nil })
	claims := token.Claims.(jwt.MapClaims)
	username := claims["username"].(string)
	nickname := getNickname(username); avatar := getAvatar(username)
	ws, _ := upgrader.Upgrade(w, r, nil); defer ws.Close()
	mu.Lock(); clients[ws] = username; onlineUsers[username] = true
	if blockedUsers[username] == nil { blockedUsers[username] = make(map[string]bool) }
	rows, _ := db.Query("SELECT blocked_username FROM blocked WHERE username = $1", username)
	defer rows.Close()
	for rows.Next() { var bu string; rows.Scan(&bu); blockedUsers[username][bu] = true }
	mu.Unlock()
	log.Printf("%s подключился. Всего: %d", username, len(clients))
	broadcastOnlineCount()
	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil { mu.Lock(); delete(clients, ws); delete(onlineUsers, username); mu.Unlock(); broadcastOnlineCount(); break }
		msg.Username = username; msg.Nickname = nickname; msg.Avatar = getAvatarURL(avatar); msg.Time = time.Now().Format("15:04")
		if msg.ChatID == 0 { msg.ChatID = 1 }
		if msg.Action == "typing" || msg.Action == "uploading_photo" || msg.Action == "uploading_video" || msg.Action == "uploading_audio" {
			mu.Lock()
			if _, ok := typingUsers[msg.ChatID]; !ok { typingUsers[msg.ChatID] = make(map[string]time.Time) }
			typingUsers[msg.ChatID][username] = time.Now()
			for c := range clients { c.WriteJSON(map[string]interface{}{"username": msg.Username, "nickname": msg.Nickname, "action": msg.Action, "chat_id": msg.ChatID, "type": "action"}) }
			mu.Unlock(); continue
		}
		sendChatNotificationIfNew(username, msg.ChatID, nickname)
		db.Exec("INSERT INTO messages (username, nickname, text, time, chat_id, avatar, read, edited, file_url, file_type, reply_to, reply_text, reply_nick) VALUES ($1, $2, $3, $4, $5, $6, false, false, $7, $8, $9, $10, $11)", msg.Username, msg.Nickname, msg.Text, msg.Time, msg.ChatID, avatar, msg.FileURL, msg.FileType, msg.ReplyTo, msg.ReplyText, msg.ReplyNick)
		broadcast <- msg
	}
}

func sendChatNotificationIfNew(username string, chatID int, nickname string) {
	if chatID == 1 { return }
	var u1, u2 string
	db.QueryRow("SELECT user1, user2 FROM chats WHERE id = $1", chatID).Scan(&u1, &u2)
	peer := u1; if peer == username { peer = u2 }
	var count int
	db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_id = $1", chatID).Scan(&count)
	if count == 0 { sendChatNotification(username, peer, chatID) }
}

func clearTypingStatuses() {
	for { time.Sleep(5 * time.Second); mu.Lock(); now := time.Now()
		for chatID, users := range typingUsers { for username, t := range users { if now.Sub(t) > 4*time.Second { delete(typingUsers[chatID], username); for c := range clients { c.WriteJSON(map[string]interface{}{"type": "typing_clear", "chat_id": chatID, "username": username}) } } } }
		mu.Unlock()
	}
}

func handleMessages() {
	for { msg := <-broadcast; mu.Lock();
		for c := range clients {
			clientUser := clients[c]
			if blockedUsers[clientUser] != nil && blockedUsers[clientUser][msg.Username] { continue }
			c.WriteJSON(map[string]interface{}{"id": msg.ID, "username": msg.Username, "nickname": msg.Nickname, "text": msg.Text, "time": msg.Time, "chat_id": msg.ChatID, "avatar": msg.Avatar, "read": msg.Read, "edited": msg.Edited, "file_url": msg.FileURL, "file_type": msg.FileType, "reply_to": msg.ReplyTo, "reply_text": msg.ReplyText, "reply_nick": msg.ReplyNick})
		}
		mu.Unlock()
	}
}

func broadcastOnlineCount() {
	mu.Lock(); c := len(clients); mu.Unlock()
	mu.Lock(); for cl := range clients { cl.WriteJSON(map[string]interface{}{"username": "system", "type": "online_count", "text": fmt.Sprintf("%d", c), "time": "online"}) }; mu.Unlock()
}