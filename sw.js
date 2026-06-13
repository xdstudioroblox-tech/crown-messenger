const CACHE_NAME = 'crown-v2';
const ASSETS = [
    '/',
    '/chat',
    '/index.html',
    '/chat.html',
    '/manifest.json'
];

self.addEventListener('install', function(event) {
    event.waitUntil(
        caches.open(CACHE_NAME).then(function(cache) {
            return cache.addAll(ASSETS).catch(function(err) {
                console.warn('⚠️ Не удалось закешировать один из ресурсов:', err);
            });
        })
    );
});

self.addEventListener('activate', function(event) {
    event.waitUntil(
        caches.keys().then(function(keys) {
            return Promise.all(
                keys.filter(function(key) { return key !== CACHE_NAME; })
                    .map(function(key) { return caches.delete(key); })
            );
        })
    );
});

self.addEventListener('fetch', function(event) {
    if (event.request.method !== 'GET') return;
    event.respondWith(
        caches.match(event.request).then(function(cached) {
            var networked = fetch(event.request)
                .then(function(response) {
                    var clone = response.clone();
                    caches.open(CACHE_NAME).then(function(cache) {
                        cache.put(event.request, clone);
                    });
                    return response;
                })
                .catch(function() {
                    return cached || new Response('Офлайн', { status: 503 });
                });
            return cached || networked;
        })
    );
});

// Push-уведомления
self.addEventListener('push', function(event) {
    var data = {};
    if (event.data) {
        try { data = event.data.json(); } catch(e) { data = { body: event.data.text() }; }
    }
    var title = data.title || 'Crown Messenger';
    var options = {
        body: data.body || 'Новое сообщение',
        icon: '/icon-192.png',
        badge: '/icon-192.png',
        vibrate: [200, 100, 200],
        data: { url: data.url || '/chat' },
        actions: [
            { action: 'open', title: 'Открыть' }
        ]
    };
    event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function(event) {
    event.notification.close();
    var url = event.notification.data && event.notification.data.url ? event.notification.data.url : '/chat';
    event.waitUntil(
        clients.matchAll({ type: 'window' }).then(function(clientList) {
            for (var i = 0; i < clientList.length; i++) {
                var client = clientList[i];
                if (client.url === url && 'focus' in client) {
                    return client.focus();
                }
            }
            if (clients.openWindow) {
                return clients.openWindow(url);
            }
        })
    );
});

// Подписка на push
self.addEventListener('message', function(event) {
    if (event.data && event.data.type === 'subscribe') {
        self.registration.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey: event.data.publicKey
        }).then(function(subscription) {
            if (event.ports && event.ports[0]) {
                event.ports[0].postMessage({ subscription: subscription });
            }
        }).catch(function(err) {
            console.log('Push subscription error:', err);
        });
    }
});