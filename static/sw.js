self.addEventListener('install', (event) => {
  console.log('Service Worker installed');
});

self.addEventListener('fetch', (event) => {
  // बस नेटवर्क से ही लाएँ (ऑफ़लाइन कैशिंग नहीं)
  event.respondWith(fetch(event.request));
});