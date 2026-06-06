package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	
	// Хранилище пользователей (пока в памяти)
	users = make(map[string]User)
	
	// Секретный ключ для JWT
	jwtSecret = []byte("super-secret-key-change-in-production")
	
	clients   = make(map[*websocket.Conn]string) // коннект -> username
	broadcast = make(chan Message)
	mu        sync.Mutex
)

type User struct {
	Nickname string `json:"nickname"`
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email,omitempty"`
}

type Message struct {
	Username string `json:"username"`
	Text     string `json:"text"`
	Time     string `json:"time"`
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

func main() {
	// API endpoints
	http.HandleFunc("/api/register", handleRegister)
	http.HandleFunc("/api/login", handleLogin)
	
	// WebSocket
	http.HandleFunc("/ws", handleConnections)
	
	// Статика
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/chat", serveChat)
	
	// Запускаем рассылку сообщений
	go handleMessages()
	
	fmt.Println("Сервер запущен на http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Ошибка запуска сервера:", err)
	}
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
	
	// Проверки
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
	
	// Проверяем, существует ли пользователь
	if _, exists := users[req.Username]; exists {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь уже существует"})
		return
	}
	
	// Сохраняем пользователя
	users[req.Username] = User{
		Nickname: req.Nickname,
		Username: req.Username,
		Password: req.Password,
		Email:    req.Email,
	}
	
	// Генерируем токен
	token, err := generateToken(req.Username)
	if err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Ошибка создания токена"})
		return
	}
	
	json.NewEncoder(w).Encode(AuthResponse{
		Success: true,
		Token:   token,
		User:    &User{Nickname: req.Nickname, Username: req.Username, Email: req.Email},
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
	
	// Проверяем пользователя
	user, exists := users[req.Username]
	if !exists {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Пользователь не найден"})
		return
	}
	
	if user.Password != req.Password {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Неверный пароль"})
		return
	}
	
	// Генерируем токен
	token, err := generateToken(req.Username)
	if err != nil {
		json.NewEncoder(w).Encode(AuthResponse{Success: false, Error: "Ошибка создания токена"})
		return
	}
	
	json.NewEncoder(w).Encode(AuthResponse{
		Success: true,
		Token:   token,
		User:    &User{Nickname: user.Nickname, Username: user.Username, Email: user.Email},
	})
}

func generateToken(username string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
	})
	
	return token.SignedString(jwtSecret)
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// Проверяем токен из query параметра
	tokenString := r.URL.Query().Get("token")
	if tokenString == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})
	
	if err != nil || !token.Valid {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}
	
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, "Invalid claims", http.StatusUnauthorized)
		return
	}
	
	username := claims["username"].(string)
	
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("Ошибка upgrade:", err)
		return
	}
	defer ws.Close()
	
	mu.Lock()
	clients[ws] = username
	mu.Unlock()
	
	log.Printf("%s подключился. Всего клиентов: %d", username, len(clients))
	
	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("Ошибка чтения от %s: %v", username, err)
			mu.Lock()
			delete(clients, ws)
			mu.Unlock()
			break
		}
		msg.Time = time.Now().Format("15:04")
		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		mu.Lock()
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("Ошибка отправки: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
		mu.Unlock()
	}
}