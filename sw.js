const CACHE_NAME = 'crown-v1';
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
            return cache.addAll(ASSETS);
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

self.addEventListener('push', function(event) {
    var data = event.data ? event.data.json() : {};
    var title = data.title || 'Crown Messenger';
    var options = {
        body: data.body || 'Новое сообщение',
        icon: '/icon-192.png',
        badge: '/icon-192.png'
    };
    event.waitUntil(self.registration.showNotification(title, options));
});