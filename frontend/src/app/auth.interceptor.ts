import { HttpInterceptorFn } from '@angular/common/http';

/** Attach JWT from login to API calls (not to /login or /signup). */
export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const token = localStorage.getItem('token');
  const isAuthRoute =
    req.url.includes('/login') || req.url.includes('/signup') || req.url.includes('/token');
  const isBackend =
    req.url.includes('127.0.0.1:8000') ||
    req.url.includes('localhost:8000');

  if (token && isBackend && !isAuthRoute) {
    return next(req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }));
  }
  return next(req);
};
